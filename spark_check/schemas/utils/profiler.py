import json
import logging
from typing import Dict, List, Optional
import os

logger = logging.getLogger(__name__)


class SchemaProfiler:
    """
    This profiler creates an "expectation" dict from an OpenAPI 3.0 schema artifact.

    Specifications: https://swagger.io/docs/specification/data-models/

    This profiler is compliant with OpenAPI 3.0 and implements:
     - array types
     - nested object types
     - string formats (date, date-time, bytes, ...)
     - numeric formats (int32, int64, ...)
    """

    def __init__(self, schema: dict, suite_name: str):
        self.schema = schema
        self.suite_name = suite_name

    def profile(self) -> dict:
        if not self.suite_name:
            raise ValueError("Please provide a suite name when using this profiler.")
        expectations = {}

        def init(column: str):
            expectations[column] = []

        def add(column: str, name: str, args: dict = None):
            expectations[column].append(
                {"name": name, "column": column, "args": args if args else {}}
            )

        def build_expectations(schema: Dict, prefix: str = None):
            if schema["type"] == "object":
                for key, details in schema["properties"].items():
                    if prefix:
                        # Object type sub-schema
                        key = f"{prefix}.{key}"

                    init(key)
                    add(key, "exists", {})

                    type_args = self._is_of_type(details)
                    if type_args:
                        add(key, "is_of_type", type_args)

                    range_expectation = self._is_in_range(details)
                    if range_expectation:
                        add(key, "is_in_range", range_expectation)

                    enum_expectation = self._is_enum(details)
                    if enum_expectation:
                        add(key, "is_enum", enum_expectation)

                    string_len_expectation = self._has_string_len(details)
                    if string_len_expectation:
                        add(key, "has_string_len", string_len_expectation)

                    is_not_null_expectation = self._is_not_null(details)
                    if is_not_null_expectation:
                        add(key, "is_not_null")

                    is_array_of_size_expectation = self._is_array_of_size(details)
                    if is_array_of_size_expectation:
                        add(key, "is_array_of_size", is_array_of_size_expectation)

                    if details.get("type") == "object":
                        build_expectations(schema=details, prefix=key)

        build_expectations(self.schema)
        return expectations

    def _get_enum_list(self, details: dict) -> Optional[List[str]]:
        enum = details.get("enum", None)
        any_of = details.get("anyOf", None)

        enum_list = []

        if enum:
            enum_list.extend(enum)
        elif any_of:
            for schema in any_of:
                enum_options = schema.get("enum", None)
                if enum_options:
                    enum_list.extend(enum_options)
        else:
            return None

        return enum_list

    def _get_type_format(self, props: dict):
        result = {key: props[key] for key in ["type", "format"] if props.get(key)}
        if "items" in props:
            result["items"] = self._get_type_format(props["items"])
        return result

    def _is_of_type(self, details: dict) -> Optional[Dict]:
        any_of = details.get("anyOf", None)

        types_list = []

        if any_of:
            for schema in any_of:
                types_list.append(self._get_type_format(schema))
        else:
            types_list.append(self._get_type_format(details))

        object_types = list(filter(lambda object_type: object_type, types_list))
        if len(object_types) == 0:
            return None
        return object_types

    def _get_range(self, props: dict):
        range_suitable_types = ["integer", "number"]
        result = {
            key: props[key]
            for key in [
                "minimum",
                "maximum",
                "exclusiveMinimum" "exclusiveMaximum",
            ]
            if props.get(key) is not None
        }

        # array case
        if "items" in props:
            items_range = self._get_range(props["items"])
            if items_range:
                result["items"] = items_range

        if result and "type" in props and props["type"] in range_suitable_types:
            result["type"] = props["type"]

        return result

    def _is_in_range(self, details: dict) -> Optional[dict]:

        any_of = details.get("anyOf", None)
        if any_of:
            for item in any_of:
                first_result = self._get_range(item)
                if first_result:
                    return first_result
        else:
            return self._get_range(details)

    def _get_str_len(self, props: dict):
        result = {
            key: props[key]
            for key in ["minLength", "maxLength"]
            if props.get(key) is not None
        }

        if result and "type" in props and props["type"] == "string":
            result["type"] = props["type"]

        return result

    def _has_string_len(self, details: dict) -> Optional[dict]:
        any_of = details.get("anyOf", None)
        if any_of:
            for item in any_of:
                first_result = self._get_str_len(item)
                if first_result:
                    return first_result
        else:
            return self._get_str_len(details)

    def _is_enum(self, details: dict) -> Optional[dict]:
        enum_list = self._get_enum_list(details=details)

        if not enum_list:
            return None
        return enum_list

    def _is_not_null(self, details: dict) -> Optional[bool]:
        nullable = details.get("nullable")
        if nullable == "true" or nullable is None:
            return None
        return True

    def _is_array_of_size(self, details: dict) -> Optional[dict]:
        result = {
            key: details[key]
            for key in ["minItems", "maxItems"]
            if details.get(key) is not None
        }

        if result and "type" in details and details["type"] == "array":
            result["type"] = "array"

        return result

    def export(self, root_path):
        with open(os.path.join(root_path, f"{self.suite_name}.json"), "w") as writer:
            writer.write(json.dumps(self.profile()))
