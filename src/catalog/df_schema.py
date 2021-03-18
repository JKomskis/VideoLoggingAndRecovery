from typing import List

from src.catalog.models.df_column import DataFrameColumn
from src.catalog.schema_utils import SchemaUtils


class DataFrameSchema(object):
    def __init__(self, name: str, column_list: List[DataFrameColumn]):

        self._name = name
        self._column_list = column_list
        self._petastorm_schema = SchemaUtils \
            .get_petastorm_schema(self._name, self._column_list)
        self._pyspark_schema = self._petastorm_schema.as_spark_schema()

    def __str__(self):
        schema_str = "SCHEMA:: (" + self._name + ")\n"
        for column in self._column_list:
            schema_str += str(column)
        return schema_str

    @property
    def name(self):
        return self._name

    @property
    def column_list(self):
        return self._column_list

    @property
    def petastorm_schema(self):
        return self._petastorm_schema

    @property
    def pyspark_schema(self):
        return self._pyspark_schema

    def __eq__(self, other):
        return self.name == other.name and \
            self._column_list == other.column_list