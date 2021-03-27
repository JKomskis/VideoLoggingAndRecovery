import unittest
import os
import cv2
import shutil

from src.transaction.transaction_manager import TransactionManager
from src.transaction.object_update_arguments import ObjectUpdateArguments
from src.catalog.models.df_metadata import DataFrameMetadata
from src.catalog.models.df_column import DataFrameColumn
from src.catalog.column_type import ColumnType
from src.models.storage.batch import Batch
from src.config.constants import TRANSACTION_STORAGE_FOLDER, INPUT_VIDEO_FOLDER

class TransactionManagerTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def test_should_create_transaction(self):
        txn_mgr = TransactionManager()
        txn_id = txn_mgr.begin_transaction()
        self.assertEqual(txn_id, 1)

        transaction_directory_path = f'{TRANSACTION_STORAGE_FOLDER}/{txn_id}'
        self.assertTrue(os.path.isdir(transaction_directory_path))
        shutil.rmtree(TRANSACTION_STORAGE_FOLDER)
    
    def test_should_create_before_and_after_image(self):
        txn_mgr = TransactionManager()
        txn_id = txn_mgr.begin_transaction()

        video_name = 'traffic001_30'
        dataframe_metadata = DataFrameMetadata(video_name, f'{INPUT_VIDEO_FOLDER}/{video_name}.mp4')
        dataframe_columns = [
            DataFrameColumn('id', ColumnType.INTEGER),
            DataFrameColumn('data', ColumnType.NDARRAY, array_dimensions= [540, 960, 3])
        ]
        dataframe_metadata.schema = dataframe_columns

        # update_operation = ObjectUpdateArguments('grayscale', 0, 300)
        # update_operation = ObjectUpdateArguments('gaussian_blur', 0, 300, ksize=(13, 13), sigmaX=0)
        update_operation = ObjectUpdateArguments('resize', 0, 300, dsize=(480, 270), interpolation=cv2.INTER_AREA)

        txn_mgr.update_object(txn_id, dataframe_metadata, update_operation)

        shutil.rmtree(TRANSACTION_STORAGE_FOLDER)

if __name__ == '__main__':
    unittest.TestLoader.sortTestMethodsUsing = None
    unittest.main()     