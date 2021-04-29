import numpy as np
import os
from typing import Iterator, List
from petastorm.codecs import CompressedImageCodec, NdarrayCodec, ScalarCodec
from petastorm.etl.dataset_metadata import materialize_dataset
from petastorm.unischema import Unischema, UnischemaField, dict_to_spark_row
from petastorm.predicates import in_lambda
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.types import IntegerType

from src.catalog.models.df_metadata import DataFrameMetadata
from src.models.storage.batch import Batch
from src.readers.petastorm_reader import PetastormReader
from src.pressure_point.pressure_point_manager import PressurePointManager
from src.pressure_point.pressure_point import PressurePoint, PressurePointLocation, PressurePointBehavior
from src.config.constants import \
    PETASTORM_STORAGE_FOLDER, \
    INPUT_VIDEO_FOLDER

class PetastormStorageEngine():
    def __init__(self):
        spark_conf = SparkConf()
        spark_conf.setMaster('local')
        spark_conf.setAppName('VLR')
        spark_conf.set('spark.logConf', 'true')
        spark_conf.set('spark.driver.memory', '8g')
        spark_conf.set('spark.sql.execution.arrow.pyspark.enabled', 'true')

        self.spark_session = SparkSession.builder.config(conf = spark_conf).getOrCreate()
        self.spark_context = self.spark_session.sparkContext  
        self.spark_context.setLogLevel('ERROR')

    def _spark_url(self, table: DataFrameMetadata):
        """
        Returns the file_url for a given file name
        file_name should be relative to the input video folder
        """
        return f'file://{os.getcwd()}/{PETASTORM_STORAGE_FOLDER}/{table.file_url}'
    
    def create(self, table: DataFrameMetadata):
        """
        Create an empty dataframe in petastorm.
        """
        empty_rdd = self.spark_context.emptyRDD()

        with materialize_dataset(self.spark_session,
                                 self._spark_url(table),
                                 table.schema.petastorm_schema):

            self.spark_session.createDataFrame(empty_rdd,
                                               table.schema.pyspark_schema) \
                .coalesce(1) \
                .write \
                .mode('overwrite') \
                .parquet(self._spark_url(table))

    def write(self, table: DataFrameMetadata, rows: Batch):
        """
        Write rows into the dataframe.
        Arguments:
            table: table metadata object to write into
            rows : batch to be persisted in the storage.
        """

        if rows.empty():
            return
        # ToDo
        # Throw an error if the row schema doesn't match the table schema

        # print(f'Table schema: {table.schema.petastorm_schema}')

        with materialize_dataset(self.spark_session,
                                 self._spark_url(table),
                                 table.schema.petastorm_schema):

            records = rows.frames
            columns = records.keys()
            rows_rdd = self.spark_context.parallelize(records.values) \
                .map(lambda x: dict(zip(columns, x))) \
                .map(lambda x: dict_to_spark_row(table.schema.petastorm_schema,
                                                 x))
            if PressurePointManager().has_pressure_point(
                PressurePoint(PressurePointLocation.PETASTORE_STORAGE_ENGINE_DURING_WRITE, PressurePointBehavior.EXCEPTION_AT_BEGINNING_OF_WRITE)):
                rows_rdd = rows_rdd.map(lambda x: None)
            elif PressurePointManager().has_pressure_point(
                PressurePoint(PressurePointLocation.PETASTORE_STORAGE_ENGINE_DURING_WRITE, PressurePointBehavior.EXCPETION_DURING_WRITE)):
                rows_rdd = rows_rdd.map(lambda x: None if x.id == 100 else x)
            self.spark_session.createDataFrame(rows_rdd,
                                               table.schema.pyspark_schema) \
                .coalesce(1) \
                .write \
                .mode('append') \
                .parquet(self._spark_url(table))

    def read(self, table: DataFrameMetadata, columns: List[
            str] = None, predicate_func=None) -> Iterator[Batch]:
        """
        Reads the table and return a batch iterator for the
        tuples that passes the predicate func.
        Argument:
            table: table metadata object to write into
            columns List[str]: A list of column names to be
                considered in predicate_func
            predicate_func: customized predicate function returns bool
        Return:
            Iterator of Batch read.
        """

        # Create a dataframe object from a parquet file
        # dataframe = self.spark_session.read.parquet(self._spark_url(table))

        # # Show a schema
        # dataframe.printSchema()

        # # Count all
        # print(dataframe.count())

        # # Show a single column
        # dataframe.select('id').show()

        predicate = None
        if predicate_func and columns:
            predicate = in_lambda(columns, predicate_func)

        # ToDo: Handle the sharding logic. We might have to maintain a
        # context for deciding which shard to read
        petastorm_reader = PetastormReader(
            self._spark_url(table), predicate=predicate)
        for batch in petastorm_reader.read():
            yield batch
