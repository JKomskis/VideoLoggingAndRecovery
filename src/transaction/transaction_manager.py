import os
import struct
import shutil
import pandas as pd

from src.catalog.models.df_metadata import DataFrameMetadata
from src.transaction.object_update_arguments import ObjectUpdateArguments
from src.transaction.transaction_metadata import TransactionMetadata
from src.storage.petastorm_storage_engine import PetastormStorageEngine
from src.models.storage.batch import Batch
from src.transaction.opencv_update_processor import OpenCVUpdateProcessor
from src.config.constants import TRANSACTION_STORAGE_FOLDER

class TransactionManager():
    def __init__(self):
        self.storage_engine = PetastormStorageEngine()
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
    
    def _get_transaction_directory(self, txn_id):
        return f'{TRANSACTION_STORAGE_FOLDER}/{txn_id}'
    
    def begin_transaction(self) -> int:
        this_txn = self._txn_counter
        self._txn_table[this_txn] = TransactionMetadata(this_txn)
        self._txn_counter = self._txn_counter + 1
        self._write_txn_counter()

        txn_directory_path = self._get_transaction_directory(this_txn)
        shutil.rmtree(txn_directory_path, ignore_errors=True)
        os.mkdir(txn_directory_path)

        # TODO: write record to log

        return this_txn
    
    def commit_transaction(self, txn_id: int):
        # TODO: write record to log
        # write changes to petastorm
        # erase transaction folder
        pass

    def abort_transaction(self, txn_id: int):
        # TODO: write record to log

        # erase transaction folder
        pass

    def update_object(self, txn_id: int, dataframe_metadata: DataFrameMetadata, update_arguments: ObjectUpdateArguments):
        # TODO: don't reread file from petastorm if version already in txn folder
        file_version = self._txn_table[txn_id].get_file_version(dataframe_metadata.file_url)
        self._txn_table[txn_id].increment_file_version(dataframe_metadata.file_url)

        before_image_file_path = f'{self._get_transaction_directory(txn_id)}/{dataframe_metadata.file_url}.v{file_version}_old'
        after_image_file_path = f'{self._get_transaction_directory(txn_id)}/{dataframe_metadata.file_url}.v{file_version}_new'
        os.makedirs(os.path.dirname(before_image_file_path))

        batch_num = 0
        for batch in self.storage_engine.read(dataframe_metadata):
            batch.frames.to_pickle(f'{before_image_file_path}_{batch_num}')

            after_image_batch = pd.DataFrame(columns=['id', 'data'])

            for index, row in batch.frames.iterrows():
                row.data = self.opencv_update_processor.apply(row.data, update_arguments)
                after_image_batch = after_image_batch.append(row)
            after_image_batch.to_pickle(f'{after_image_file_path}_{batch_num}')

            batch_num = batch_num + 1
        
        # TODO: write log record to file
        # Write change to petastorm
        pass