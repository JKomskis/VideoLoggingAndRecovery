import unittest
import os
import cv2
import shutil
import pandas as pd

from pandas.testing import assert_frame_equal
from src.transaction.optimized_transaction_manager import OptimizedTransactionManager
from src.transaction.object_update_arguments import ObjectUpdateArguments
from test.utils.util_functions import ignore_warnings, \
                                        write_file, \
                                        read_file_from_fs, \
                                        apply_update_to_dataframe, \
                                        read_file_from_image, \
                                        read_file_from_petastorm, \
                                        dataframes_equal, \
                                        clear_petastorm_storage_folder
from src.storage.partitioned_petastorm_storage_engine import PartitionedPetastormStorageEngine
from src.Logging.logical_log_manager import LogicalLogManager
from src.buffer.buffer_manager import BufferManager
from src.utils.logging_manager import LoggingLevel, LoggingManager
from src.config.constants import TRANSACTION_STORAGE_FOLDER

class OptimizedTransactionManagerTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.storage_engine = PartitionedPetastormStorageEngine()
        LoggingManager().setEffectiveLevel(LoggingLevel.DEBUG)
    
    def tearDown(self):
        clear_petastorm_storage_folder()

    def test_should_create_transaction(self):
        log_mgr = LogicalLogManager()
        buffer_mgr = BufferManager(200, self.storage_engine)
        txn_mgr = OptimizedTransactionManager(storage_engine_passed=self.storage_engine,
                                    log_manager_passed=log_mgr,
                                    buffer_manager_passed=buffer_mgr)
        txn_id = txn_mgr.begin_transaction()
        self.assertEqual(txn_id, 1)

        transaction_directory_path = f'{TRANSACTION_STORAGE_FOLDER}/{txn_id}'
        self.assertTrue(os.path.isdir(transaction_directory_path))
    
    @ignore_warnings
    def test_should_update_video_in_buffer_manager(self):
        update_operation = ObjectUpdateArguments('invert_color', 0, 299)

        dataframe_metadata = write_file(self.storage_engine, 'traffic001_6')

        video_frames = read_file_from_petastorm(self.storage_engine, dataframe_metadata)
        expected_updated_video_frames = apply_update_to_dataframe(video_frames, update_operation)

        log_mgr = LogicalLogManager()
        buffer_mgr = BufferManager(200, self.storage_engine)
        txn_mgr = OptimizedTransactionManager(storage_engine_passed=self.storage_engine,
                                    log_manager_passed=log_mgr,
                                    buffer_manager_passed=buffer_mgr)
        txn_id = txn_mgr.begin_transaction()
        txn_mgr.update_object(txn_id, dataframe_metadata, update_operation)

        actual_updated_video_frames = pd.DataFrame()
        for i in range(4):
            batch = buffer_mgr.read_slot(dataframe_metadata, i)
            actual_updated_video_frames = actual_updated_video_frames.append(batch.frames, ignore_index=True)

        LoggingManager().log(f'Asserting buffer manager is updated', LoggingLevel.INFO)
        self.assertTrue(dataframes_equal(expected_updated_video_frames, actual_updated_video_frames))

    # @ignore_warnings
    # def test_should_revert_changes_on_abort(self):
    #     dataframe_metadata = write_file(self.storage_engine, 'traffic001_6')
    #     initial_video_frames = read_file_from_petastorm(self.storage_engine, dataframe_metadata)

    #     txn_mgr = TransactionManager(storage_engine_passed=self.storage_engine)
    #     txn_id = txn_mgr.begin_transaction()

    #     update_operation = ObjectUpdateArguments('grayscale', 0, 300)
    #     txn_mgr.update_object(txn_id, dataframe_metadata, update_operation)
    #     after_image = read_file_from_image(f'{txn_mgr.get_transaction_directory(txn_id)}/{dataframe_metadata.file_url}.v0_new')

    #     txn_mgr.abort_transaction(txn_id)

    #     updated_video_frames = read_file_from_petastorm(self.storage_engine, dataframe_metadata)
    #     self.assertTrue(dataframes_equal(initial_video_frames, updated_video_frames))
    #     self.assertFalse(dataframes_equal(updated_video_frames, after_image))


if __name__ == '__main__':
    unittest.main()     
