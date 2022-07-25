import copy


def resolve_schema(schema_object: dict):
    """
    Takes a valid open api 3.0 or jsonschema object and resolves all `$ref`
    listed in an `allOf` field as specified in:
        - https://swagger.io/docs/specification/using-ref/
    :param schema_object:
    :return: schema_object
    """
    __somewhat_validate(schema_object)
    schema = copy.deepcopy(schema_object)
    __resolve_allOf(schema)
    return schema


def __resolve_allOf(schema_object: dict):
    for schema in schema_object["components"]["schemas"].values():
        if "allOf" in schema:
            if "properties" not in schema:
                schema["properties"] = {}
            additional_properties = {}
            for path in schema["allOf"]:
                properties = __get_ref(schema_object, path["$ref"])["properties"]
                additional_properties = {**additional_properties, **properties}
            schema["properties"] = {**additional_properties, **schema["properties"]}
            schema.pop("allOf")


def __get_ref(root: dict, ref: str):
    """
    see https://swagger.io/docs/specification/using-ref/
    For a given root dictionary and a json $ref as "#/foo/bar",
    returns value at root["foo"]["bar"]
    :param root:
    :param ref:
    :return: expected_value
    """
    url, path = ref.split("#")
    expected_value = copy.deepcopy(root)
    traversed_keys = list(filter(None, path.split("/")))
    for key in traversed_keys:
        expected_value = expected_value[key]
    return expected_value


def __somewhat_validate(schema_object: dict):
    assert "components" in schema_object
    assert "schemas" in schema_object["components"]
