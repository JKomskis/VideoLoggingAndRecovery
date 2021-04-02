import pandas as pd
import warnings
import glob
import numpy as np

from src.catalog.models.df_metadata import DataFrameMetadata
from src.catalog.models.df_column import DataFrameColumn
from src.catalog.column_type import ColumnType
from src.models.storage.batch import Batch
from src.storage.petastorm_storage_engine import PetastormStorageEngine
from src.readers.opencv_reader import OpenCVReader
from src.transaction.opencv_update_processor import OpenCVUpdateProcessor
from src.transaction.object_update_arguments import ObjectUpdateArguments
from src.utils.logging_manager import LoggingLevel, LoggingManager
from src.config.constants import TRANSACTION_STORAGE_FOLDER, \
                                 INPUT_VIDEO_FOLDER, \
                                 PETASTORM_STORAGE_FOLDER

def ignore_warnings(test_func):
    def do_test(self, *args, **kwargs):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ResourceWarning)
            warnings.simplefilter("ignore", DeprecationWarning)
            test_func(self, *args, **kwargs)
    return do_test

def write_file(storage_engine, file_name) -> DataFrameMetadata:
    LoggingManager().log(f'Writing file {file_name}', LoggingLevel.INFO)
    dataframe_metadata = DataFrameMetadata(file_name, f'{INPUT_VIDEO_FOLDER}/{file_name}.mp4')

    reader = OpenCVReader(file_url = dataframe_metadata.file_url, batch_size = 50)

    dataframe_columns = [
        DataFrameColumn('id', ColumnType.INTEGER),
        DataFrameColumn('data', ColumnType.NDARRAY, array_dimensions= [reader.video_height(), reader.video_width(), 3])
    ]
    dataframe_metadata.schema = dataframe_columns

    storage_engine.create(dataframe_metadata)

    for batch in reader.read():
        storage_engine.write(dataframe_metadata, batch)
    LoggingManager().log(f'Done writing file {file_name}', LoggingLevel.INFO)
    return dataframe_metadata

def read_file_from_fs(file_name: str) -> pd.DataFrame:
    df = None
    first_batch = True
    reader = OpenCVReader(file_url = f'{INPUT_VIDEO_FOLDER}/{file_name}.mp4', batch_size = 50)
    for batch in reader.read():
        if first_batch:
            first_batch = False
            df = batch.frames
        else:
            df = df.append(batch.frames, ignore_index=True)
    return df

def read_file_from_image(file_path: str) -> pd.DataFrame:
    df = None
    first_batch = True
    LoggingManager().log(f'Reading image {file_path}', LoggingLevel.INFO)
    for name in sorted(glob.glob(f'{file_path}_*')):
        LoggingManager().log(f'Reading batch: {name}', LoggingLevel.INFO)
        batch = pd.read_pickle(name)
        if first_batch:
            first_batch = False
            df = batch
        else:
            df = df.append(batch, ignore_index=True)
    return df

def read_file_from_petastorm(storage_engine: PetastormStorageEngine, dataframe_metadata: DataFrameMetadata) -> pd.DataFrame:
    df = None
    first_batch = True
    for batch in storage_engine.read(dataframe_metadata):
        if first_batch:
            first_batch = False
            df = batch.frames
        else:
            df = df.append(batch.frames, ignore_index=True)
    return df

def apply_update_to_dataframe(df: pd.DataFrame, update_args: ObjectUpdateArguments) -> pd.DataFrame:
    updater = OpenCVUpdateProcessor()
    new_df = df.copy(deep=True)
    for index, row in new_df.iterrows():
        new_df.at[index, 'data'] = updater.apply(row['data'], update_args)
    return new_df

def dataframes_equal(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
    if df1['id'].max() != df2['id'].max():
        LoggingManager().log(f'Dataframes have different sizes', LoggingLevel.DEBUG)
        return False
    
    max_frame = df1['id'].max()
    i = 0
    while i <= max_frame:
        if not np.array_equal(df1.loc[df1['id'] == i].iloc[0].data,
                              df2.loc[df2['id'] == i].iloc[0].data):
            LoggingManager().log(f'Dataframes differ on frame {i}', LoggingLevel.DEBUG)
            return False
        i = i+1
    LoggingManager().log(f'Dataframes are identical', LoggingLevel.DEBUG)
    return True