from typing import List
import pandas as pd
import glob

from src.buffer.buffer_manager import BufferManager
from src.catalog.models.df_metadata import DataFrameMetadata
from src.models.storage.batch import Batch
from src.transaction.object_update_arguments import ObjectUpdateArguments
from src.transaction.opencv_update_processor import OpenCVUpdateProcessor
from src.readers.partitioned_petastorm_reader import GroupDoesNotExistException
from src.config.constants import BATCH_SIZE
from src.utils.logging_manager import LoggingManager, LoggingLevel

def apply_object_update_arguments_to_buffer_manager(buffer_manager: BufferManager,
                                                    opencv_update_processor: OpenCVUpdateProcessor,
                                                    dataframe_metadata: DataFrameMetadata,
                                                    update_arguments: ObjectUpdateArguments,
                                                    lsn: int):
    start_group = int(update_arguments.start_frame // BATCH_SIZE)
    end_group = int(update_arguments.end_frame // BATCH_SIZE)
    curr_group = start_group
    while curr_group <= end_group:
        try:
            batch = buffer_manager.read_slot(dataframe_metadata, curr_group)

            LoggingManager().log(f'lsn: {lsn} max_lsn: {batch.frames["lsn"].max()}', LoggingLevel.DEBUG)

            if lsn > batch.frames['lsn'].max():
                new_df = pd.DataFrame()
                for index, row in batch.frames.iterrows():
                    if update_arguments.start_frame <= row.id and update_arguments.end_frame >= row.id:
                        row.data = opencv_update_processor.apply(row.data, update_arguments)
                        row.lsn = lsn
                        new_df = new_df.append(row, ignore_index=True)
                new_batch = Batch(new_df)

                buffer_manager.write_slot(dataframe_metadata, new_batch)
            curr_group = curr_group + 1
        except GroupDoesNotExistException as e:
            break

def apply_before_deltas_to_buffer_manager(buffer_manager: BufferManager,
                                            dataframe_metadata: DataFrameMetadata,
                                            before_delta_path: str,
                                            lsn: int):
    for path in glob.glob(f'{before_delta_path}_*'):
        try:
            curr_group = int(path[path.rfind('_')+1:])
            batch = buffer_manager.read_slot(dataframe_metadata, curr_group)

            LoggingManager().log(f'lsn: {lsn} max_lsn: {batch.frames["lsn"].max()}', LoggingLevel.DEBUG)

            if lsn > batch.frames['lsn'].max():
                orig_df = pd.read_pickle(path)
                orig_df['lsn'] = lsn
                orig_batch = Batch(orig_df)

                buffer_manager.write_slot(dataframe_metadata, orig_batch)
            curr_group = curr_group + 1
        except GroupDoesNotExistException as e:
            break