import pandas as pd
import warnings
import glob
import numpy as np
import os
import shutil
import cv2

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
                                 PETASTORM_STORAGE_FOLDER, \
                                 BATCH_SIZE

def ignore_warnings(test_func):
    def do_test(self, *args, **kwargs):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ResourceWarning)
            warnings.simplefilter("ignore", DeprecationWarning)
            test_func(self, *args, **kwargs)
    return do_test

def write_file(storage_engine, file_name, include_lsn=False) -> DataFrameMetadata:
    LoggingManager().log(f'Writing file {file_name}', LoggingLevel.INFO)
    dataframe_metadata = DataFrameMetadata(file_name, f'{INPUT_VIDEO_FOLDER}/{file_name}.mp4')

    reader = OpenCVReader(file_url = dataframe_metadata.file_url, include_lsn=include_lsn, batch_size = BATCH_SIZE)

    dataframe_columns = [
        DataFrameColumn('id', ColumnType.INTEGER),
        DataFrameColumn('data', ColumnType.NDARRAY, array_dimensions= [reader.video_height(), reader.video_width(), 3])
    ]
    if include_lsn:
        dataframe_columns.append(DataFrameColumn('lsn', ColumnType.INTEGER))
    dataframe_metadata.schema = dataframe_columns

    storage_engine.create(dataframe_metadata)

    for batch in reader.read():
        storage_engine.write(dataframe_metadata, batch)
    LoggingManager().log(f'Done writing file {file_name}', LoggingLevel.INFO)
    return dataframe_metadata

def read_file_from_fs(file_name: str, include_lsn=False) -> pd.DataFrame:
    df = None
    first_batch = True
    reader = OpenCVReader(file_url = f'{INPUT_VIDEO_FOLDER}/{file_name}.mp4', include_lsn=include_lsn, batch_size = BATCH_SIZE)
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

def read_file_from_petastorm(storage_engine: PetastormStorageEngine, dataframe_metadata: DataFrameMetadata, group_num=None) -> pd.DataFrame:
    df = None
    first_batch = True
    read_result = None
    if group_num == None:
        read_result = storage_engine.read(dataframe_metadata)
    else:
        read_result = storage_engine.read(dataframe_metadata, group_num=group_num)
    for batch in read_result:
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
        if update_args.start_frame <= row.id and update_args.end_frame >= row.id:
            new_df.at[index, 'data'] = updater.apply(row['data'], update_args)
    return new_df

def apply_update_to_dataframe_delta(df: pd.DataFrame, update_args: ObjectUpdateArguments) -> pd.DataFrame:
    updater = OpenCVUpdateProcessor()
    new_df = pd.DataFrame()
    for index, row in df.iterrows():
        if update_args.start_frame <= row.id and update_args.end_frame >= row.id:
            row.data = updater.apply(row.data, update_args)
            new_df = new_df.append(row, ignore_index=True)
    return new_df

def dataframes_equal(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
    if df1['id'].max() != df2['id'].max():
        LoggingManager().log(f'Dataframes have different sizes', LoggingLevel.DEBUG)
        return False
    
    max_frame = df1['id'].max()
    i = df1['id'].min()
    while i <= max_frame:
        if not np.array_equal(df1.loc[df1['id'] == i].iloc[0].data,
                              df2.loc[df2['id'] == i].iloc[0].data):
            LoggingManager().log(f'Dataframes differ on frame {i}', LoggingLevel.DEBUG)
            return False
        i = i+1
    LoggingManager().log(f'Dataframes are identical', LoggingLevel.DEBUG)
    return True

def clear_folder(dir_name: str):
    for filename in os.listdir(dir_name):
        file_path = os.path.join(dir_name, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)

def clear_petastorm_storage_folder():
    clear_folder(PETASTORM_STORAGE_FOLDER)

def clear_transaction_storage_folder():
    clear_folder(TRANSACTION_STORAGE_FOLDER)

def write_dataframe_to_video(df: pd.DataFrame, output_file_path: str) -> None:
    output_df = df.sort_values(by='id')

    width = output_df.data.iloc[0].shape[1]
    height = output_df.data.iloc[0].shape[0]
    print(f'({width}, {height})')
    fps = 30
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    output = cv2.VideoWriter(output_file_path, fourcc, fps, (width, height))

    for frame in output_df.data:
        output.write(frame)
    output.release()