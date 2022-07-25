"""
See: https://swagger.io/docs/specification/data-models/data-types/
"""
from pyspark.sql import types
from typing import Optional


def spark_mapping(properties: dict) -> Optional[types.DataType]:
    """
    Maps OpenAPI 3.0 data types to Spark DataType
    :param properties:
    :return: Optional[DataType]
    """

    _type = properties.get("type")
    _format = properties.get("format")  # Optional
    _items = properties.get("items")  # Mandatory for ArrayType

    if _type == "string":
        if _format == "date":
            return types.DateType()
        if _format == "date-time":
            return types.TimestampType()
        if _format == "byte":
            return types.ByteType()
        if _format == "binary":
            return types.BinaryType()
        else:
            return types.StringType()

    if _type == "number":
        if _format == "float":
            return types.FloatType()
        if _format == "double":
            return types.DoubleType()
        else:
            return types.DoubleType()

    if _type == "integer":
        if _format == "int32":
            return types.IntegerType()
        if _format == "int64":
            return types.LongType()
        else:
            return types.LongType()

    if _type == "boolean":
        return types.BooleanType()

    if _type == "array":
        spark_type = spark_mapping(_items)
        if spark_type:
            return types.ArrayType(spark_type)

    return None
