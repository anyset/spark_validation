"""
Conditions (Column, params) -> Column
- Applies a condition to a column and returns a BooleanType Column
"""
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql import types as T
from validation.types_mapping import spark_mapping
from validation.spark_decorator import validation
from pyspark.sql.functions import lit, udf


@validation
def is_not_null(field: Column, params) -> Column:
    return field.isNotNull()


@validation
def is_enum(field: Column, params: list):
    result = lit(True)
    for param in params:
        result = result & (field == lit(param))
    return result


@validation
def is_of_type(field: Column, params: list):
    @udf(T.BooleanType())
    def validate_array(array):
        return False if None in array else True

    result = lit(False)
    for tpe in params:
        spark_type = spark_mapping(tpe)
        if tpe["type"] == "array":
            result = result | validate_array(field.cast(spark_type))
        elif spark_type:
            result = result | field.cast(spark_type).isNotNull()
    return result


@validation
def has_string_len(field: Column, params: dict):
    result = lit(True)
    for b in list(params.keys()):
        if b == "minLength":
            result = result & (F.length(field) >= lit(params[b]))
        elif b == "maxLength":
            result = result & (F.length(field) <= lit(params[b]))
    return result


@validation
def is_array_of_size(field: Column, params: dict):
    result = lit(True)

    for b in list(params.keys()):
        if b == "minItems":
            result = result & (F.size(field) >= lit(params[b]))
        elif b == "maxItems":
            result = result & (F.size(field) <= lit(params[b]))
    return result


@validation
def is_in_range(field: Column, params: dict):
    result = lit(True)

    # Case array - note we don't do nested arrays yet
    if "items" in params.keys():
        for b in params["items"].keys():
            if b == "minimum":
                result = result & (F.array_min(field) >= lit(params[b]))
            elif b == "maximum":
                result = result & (F.array_max(field) <= lit(params[b]))
            elif b == "exclusiveMinimum":
                result = result & (F.array_min(field) > lit(params[b]))
            elif b == "exclusiveMaximum":
                result = result & (F.array_max(field) < lit(params[b]))

    else:
        for b in list(params.keys()):
            if b == "minimum":
                result = result & (field >= lit(params[b]))
            elif b == "maximum":
                result = result & (field <= lit(params[b]))
            elif b == "exclusiveMinimum":
                result = result & (field > lit(params[b]))
            elif b == "exclusiveMaximum":
                result = result & (field < lit(params[b]))

    return result


# Let's have some...
fun = locals()
