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
from src.storage.petastorm_storage_engine import PetastormStorageEngine
from src.models.storage.batch import Batch
from src.transaction.opencv_update_processor import OpenCVUpdateProcessor
from src.config.constants import TRANSACTION_STORAGE_FOLDER, INPUT_VIDEO_FOLDER
from src.utils.logging_manager import LoggingLevel, LoggingManager
from src.Logging.log_manager import LogManager

class TransactionManager():
    def __init__(self, storage_engine_passed=None):
        if storage_engine_passed != None:
            self.storage_engine = storage_engine_passed
        else:
            self.storage_engine = PetastormStorageEngine()
        self.opencv_update_processor = OpenCVUpdateProcessor()
        self.log_manager = LogManager()
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
            LoggingManager().log(f'Writing batch: {batch_path}', LoggingLevel.DEBUG)

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

        # erase transaction folder
        shutil.rmtree(self.get_transaction_directory(txn_id))

    def abort_transaction(self, txn_id: int):
        # file_urls = self._txn_table[txn_id].get_updated_files()
        # for file_url in file_urls:
        #     image_path = f'{self.get_transaction_directory(txn_id)}/{file_url}.v0_old'
        #     self.write_serialized_image(file_url, image_path)

        for (file_url, after_path, before_path) in self.log_manager.rollback_txn(txn_id):
            self.write_serialized_image(file_url, before_path)

        self.log_manager.log_abort_txn_record(txn_id)

        # erase transaction folder
        shutil.rmtree(self.get_transaction_directory(txn_id))

    def update_object(self, txn_id: int, dataframe_metadata: DataFrameMetadata, update_arguments: ObjectUpdateArguments):
        # Write before and after images to file system
        file_version = self._txn_table[txn_id].get_file_version(dataframe_metadata.file_url)
        self._txn_table[txn_id].increment_file_version(dataframe_metadata.file_url)

        before_image_file_path = f'{self.get_transaction_directory(txn_id)}/{dataframe_metadata.file_url}.v{file_version}_old'
        after_image_file_path = f'{self.get_transaction_directory(txn_id)}/{dataframe_metadata.file_url}.v{file_version}_new'
        os.makedirs(os.path.dirname(before_image_file_path), exist_ok=True)

        batch_num = 0
        for batch in self.storage_engine.read(dataframe_metadata):
            batch.frames.to_pickle(f'{before_image_file_path}_{batch_num}')

            after_image_batch = pd.DataFrame(columns=['id', 'data'])

            for index, row in batch.frames.iterrows():
                if update_arguments.start_frame <= row.id and update_arguments.end_frame > row.id:
                    row.data = self.opencv_update_processor.apply(row.data, update_arguments)
                after_image_batch = after_image_batch.append(row, ignore_index=True)
            after_image_batch.to_pickle(f'{after_image_file_path}_{batch_num}')

            batch_num = batch_num + 1

        self.log_manager.log_update_record(
            txn_id,
            dataframe_metadata.file_url,
            before_image_file_path,
            after_image_file_path
        )

        # Write change to petastorm
        self.write_serialized_image(dataframe_metadata.file_url, after_image_file_path)

    def recover(self):
        self.log_manager.recover_log()
