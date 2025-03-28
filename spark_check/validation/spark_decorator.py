from pyspark.sql import Column
from pyspark.sql.functions import col, lit, when, udf
from pyspark.sql.types import BooleanType, StringType

import logging


logger = logging.getLogger(__name__)


def validation(condition):
    def decorator(field: str, result_column: str, params) -> Column:
        """
        Decorates a condition (AnyType -> BooleanType) to return a "when" clause Column
        (which creates a new column based on said condition)
        :param field:
        :param result_column:
        :param params:
        :return: Column
        """
        name = condition.__name__
        logger.info(f"Evaluating {name} on col {field}...")

        return (
            when(condition(col(field), params), lit(""))
            .otherwise(
                lit(
                    f"{{"
                    f"'field': '{field}', "
                    f"'condition': '{name}', "
                    f"'expected': {str(params)}}}"
                )
            )
            .alias(result_column)
        )

    return decorator


@udf(BooleanType())
def get_success(results: list):
    failures = [result for result in results if result]
    if len(failures) > 0:
        return False
    return True


@udf(StringType())
def get_report(results: list):
    failures = [result for result in results if result]
    return "[" + ", ".join(failures) + "]"
