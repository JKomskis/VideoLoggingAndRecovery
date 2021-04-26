import os
import struct
import shutil
import pandas as pd
import glob
from pathlib import Path

from src.catalog.models.df_metadata import DataFrameMetadata
from src.catalog.models.df_column import DataFrameColumn
from src.catalog.column_type import ColumnType
from src.transaction.object_update_arguments import ObjectUpdateArguments
from src.transaction.transaction_metadata import TransactionMetadata
from src.storage.partitioned_petastorm_storage_engine import PartitionedPetastormStorageEngine
from src.readers.partitioned_petastorm_reader import GroupDoesNotExistException
from src.models.storage.batch import Batch
from src.transaction.opencv_update_processor import OpenCVUpdateProcessor
from src.Logging.logical_log_manager import LogicalLogManager
from src.buffer.buffer_manager import BufferManager
from src.config.constants import TRANSACTION_STORAGE_FOLDER, INPUT_VIDEO_FOLDER, BATCH_SIZE
from src.utils.logging_manager import LoggingLevel, LoggingManager

class OptimizedTransactionManager():
    def __init__(self, storage_engine_passed=None, log_manager_passed=None, buffer_manager_passed=None):
        if storage_engine_passed != None:
            self.storage_engine = storage_engine_passed
        else:
            self.storage_engine = PartitionedPetastormStorageEngine()
        
        if log_manager_passed != None:
            self.log_manager = log_manager_passed
        else:
            self.log_manager = LogicalLogManager()
        
        if buffer_manager_passed != None:
            self.buffer_manager = buffer_manager_passed
        else:
            self.buffer_manager = BufferManager(200, self.storage_engine)

        self.opencv_update_processor = OpenCVUpdateProcessor()
        self._txn_table = {}
        self._txn_counter_file_path = f'{TRANSACTION_STORAGE_FOLDER}/txn_counter'
        self._txn_counter = 1

        if not os.path.isdir(TRANSACTION_STORAGE_FOLDER):
            os.mkdir(TRANSACTION_STORAGE_FOLDER)

        if not os.path.isfile(self._txn_counter_file_path):
            self._write_txn_counter()
        else:
            with open(self._txn_counter_file_path, 'rb') as txn_counter_file:
                self._txn_counter = struct.unpack('i', txn_counter_file.read())[0]
    
    def _write_txn_counter(self):
        with open(self._txn_counter_file_path, 'wb') as txn_counter_file:
            txn_counter_file.write(struct.pack('i', self._txn_counter))
    
    def get_transaction_directory(self, txn_id):
        return f'{TRANSACTION_STORAGE_FOLDER}/{txn_id}'
    
    def write_serialized_image(self, file_url, image_path):
        LoggingManager().log(f'Writing image {image_path}', LoggingLevel.INFO)
        dataframe_metadata = None
        first_batch = True
        for batch_path in sorted(glob.glob(f'{image_path}_*')):
            frames_df = pd.read_pickle(batch_path)
            LoggingManager().log(f'Writing batch: {batch_path}', LoggingLevel.INFO)

            if first_batch:
                first_batch = False
                width = frames_df.data.iloc[0].shape[1]
                height = frames_df.data.iloc[0].shape[0]
                dataframe_metadata = DataFrameMetadata(Path(file_url).stem, file_url)
                dataframe_columns = [
                    DataFrameColumn('id', ColumnType.INTEGER),
                    DataFrameColumn('data', ColumnType.NDARRAY, array_dimensions= [height, width, 3])
                ]
                dataframe_metadata.schema = dataframe_columns

                self.storage_engine.create(dataframe_metadata)

            self.storage_engine.write(dataframe_metadata, Batch(frames_df))
    
    def begin_transaction(self) -> int:
        this_txn = self._txn_counter
        self._txn_table[this_txn] = TransactionMetadata(this_txn)
        self._txn_counter = self._txn_counter + 1
        self._write_txn_counter()

        txn_directory_path = self.get_transaction_directory(this_txn)
        shutil.rmtree(txn_directory_path, ignore_errors=True)
        os.mkdir(txn_directory_path)

        self.log_manager.log_begin_txn_record(this_txn)

        return this_txn
    
    def commit_transaction(self, txn_id: int):
        self.log_manager.log_commit_txn_record(txn_id)

    def abort_transaction(self, txn_id: int):
        self.log_manager.log_abort_txn_record(txn_id)

        # TODO: rollback transaction


    def update_object(self, txn_id: int, dataframe_metadata: DataFrameMetadata, update_arguments: ObjectUpdateArguments):
        if self.opencv_update_processor.is_reversible(update_arguments):
            # Do logical logging
            # Write log record to file
            self.log_manager.log_logical_update_record(txn_id, dataframe_metadata.file_url, update_arguments)

            # Apply update through the buffer manager
            start_group = int(update_arguments.start_frame // BATCH_SIZE)
            end_group = int(update_arguments.end_frame // BATCH_SIZE)
            curr_group = start_group
            while curr_group <= end_group:
                try:
                    batch = self.buffer_manager.read_slot(dataframe_metadata, curr_group)

                    new_df = pd.DataFrame()
                    for index, row in batch.frames.iterrows():
                        if update_arguments.start_frame <= row.id and update_arguments.end_frame >= row.id:
                            row.data = self.opencv_update_processor.apply(row.data, update_arguments)
                            new_df = new_df.append(row, ignore_index=True)
                    new_batch = Batch(new_df)

                    self.buffer_manager.write_slot(dataframe_metadata, new_batch)
                    curr_group = curr_group + 1
                except GroupDoesNotExistException as e:
                    break
                
        else:
            # Fallback to physical logging
            pass
