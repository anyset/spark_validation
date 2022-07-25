import logging

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import Column, array

from validation.spark_conditions import fun
from validation.spark_decorator import get_report, get_success


class FieldValidator:
    def __init__(self, expectation: dict):
        self.logger = logging.getLogger(__name__)
        self.expectation = expectation

    @property
    def fun(self):
        return fun[self.expectation["name"]]

    @property
    def column(self) -> str:
        return self.expectation["column"]

    @property
    def result_column(self):
        return f'{self.expectation["column"]}_{self.expectation["name"]}'

    @property
    def params(self) -> dict:
        return self.expectation.get("params", {})

    @property
    def clause(self) -> Column:
        return self.fun(self.column, self.result_column, self.params)


class Validator:
    def __init__(self, expectations: dict):
        self._logger = logging.getLogger(__name__)
        self._expectations = expectations

    def __get_validators(self):

        validators = {}
        for column in self._expectations.keys():
            column_expectations = self._expectations[column]
            validators[column] = []
            for expectation in column_expectations:
                if self.is_valid_expectation(expectation):
                    validators[column].append(FieldValidator(expectation))
        return validators

    @staticmethod
    def is_valid_expectation(expectation):
        condition = (
            "name" in expectation
            and expectation["name"] in fun.keys()
            and "column" in expectation
        )
        return True if condition else False

    def validate_all(self, df: DataFrame):
        validators = self.__get_validators()
        origin_columns = df.columns
        clauses = []
        results = []

        for field in validators.keys():
            for validator in validators[field]:
                clauses.append(validator.clause)
                results.append(validator.result_column)

        # Compute clauses Columns
        df = df.select(*origin_columns, *clauses)

        # Merge clauses Columns to success and result Columns
        df = df.select(
            *origin_columns,
            get_success(array(*set(results))).alias("success"),
            get_report(array(*set(results))).alias("report"),
        )

        return df
