import numpy as np
from petastorm.codecs import NdarrayCodec
from petastorm.codecs import ScalarCodec
from petastorm.unischema import Unischema
from petastorm.unischema import UnischemaField
from pyspark.sql.types import IntegerType, FloatType, StringType

from src.catalog.column_type import ColumnType
from src.utils.logging_manager import LoggingLevel
from src.utils.logging_manager import LoggingManager


class SchemaUtils(object):

    @staticmethod
    def get_petastorm_column(df_column):

        column_type = df_column.type
        column_name = df_column.name
        column_is_nullable = df_column.is_nullable
        column_array_dimensions = df_column.array_dimensions

        # Reference:
        # https://github.com/uber/petastorm/blob/master/petastorm/
        # tests/test_common.py

        petastorm_column = None
        if column_type == ColumnType.INTEGER:
            petastorm_column = UnischemaField(column_name,
                                              np.int32,
                                              (),
                                              ScalarCodec(IntegerType()),
                                              column_is_nullable)
        elif column_type == ColumnType.FLOAT:
            petastorm_column = UnischemaField(column_name,
                                              np.float64,
                                              (),
                                              ScalarCodec(FloatType()),
                                              column_is_nullable)
        elif column_type == ColumnType.TEXT:
            petastorm_column = UnischemaField(column_name,
                                              np.str_,
                                              (),
                                              ScalarCodec(StringType()),
                                              column_is_nullable)
        elif column_type == ColumnType.NDARRAY:
            petastorm_column = UnischemaField(column_name,
                                              np.uint8,
                                              column_array_dimensions,
                                              NdarrayCodec(),
                                              column_is_nullable)
        else:
            LoggingManager().log("Invalid column type: " + str(column_type),
                                 LoggingLevel.ERROR)

        return petastorm_column

    @staticmethod
    def get_petastorm_schema(name, column_list):
        petastorm_column_list = []
        for _column in column_list:
            petastorm_column = SchemaUtils.get_petastorm_column(_column)
            petastorm_column_list.append(petastorm_column)

        petastorm_schema = Unischema(name, petastorm_column_list)
        return petastorm_schema