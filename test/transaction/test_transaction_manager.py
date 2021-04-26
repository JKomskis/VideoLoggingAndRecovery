import unittest
import os
import cv2
import shutil

from pandas.testing import assert_frame_equal
from src.transaction.transaction_manager import TransactionManager
from src.transaction.object_update_arguments import ObjectUpdateArguments
from test.utils.util_functions import ignore_warnings, \
                                        write_file, \
                                        read_file_from_fs, \
                                        apply_update_to_dataframe, \
                                        read_file_from_image, \
                                        read_file_from_petastorm, \
                                        dataframes_equal, \
                                        clear_petastorm_storage_folder
from src.storage.petastorm_storage_engine import PetastormStorageEngine
from src.utils.logging_manager import LoggingLevel, LoggingManager
from src.config.constants import TRANSACTION_STORAGE_FOLDER

class TransactionManagerTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.storage_engine = PetastormStorageEngine()
        LoggingManager().setEffectiveLevel(LoggingLevel.DEBUG)
    
    def tearDown(self):
        clear_petastorm_storage_folder()

    def test_should_create_transaction(self):
        txn_mgr = TransactionManager(storage_engine_passed=self.storage_engine)
        txn_id = txn_mgr.begin_transaction()
        self.assertEqual(txn_id, 1)

        transaction_directory_path = f'{TRANSACTION_STORAGE_FOLDER}/{txn_id}'
        self.assertTrue(os.path.isdir(transaction_directory_path))
    
    @ignore_warnings
    def test_should_create_before_and_after_image(self):
        update_operation = ObjectUpdateArguments('grayscale', 0, 300)
        # update_operation = ObjectUpdateArguments('gaussian_blur', 0, 300, ksize=(13, 13), sigmaX=0)
        # update_operation = ObjectUpdateArguments('resize', 0, 300, dsize=(480, 270), interpolation=cv2.INTER_AREA)

        dataframe_metadata = write_file(self.storage_engine, 'traffic001_6')

        video_frames = read_file_from_petastorm(self.storage_engine, dataframe_metadata)
        updated_video_frames = apply_update_to_dataframe(video_frames, update_operation)
        LoggingManager().log(f'Asserting videos_frames different from updated_video_frames', LoggingLevel.INFO)
        self.assertFalse(dataframes_equal(video_frames, updated_video_frames))

        txn_mgr = TransactionManager(storage_engine_passed=self.storage_engine)
        txn_id = txn_mgr.begin_transaction()
        txn_mgr.update_object(txn_id, dataframe_metadata, update_operation)

        before_image = read_file_from_image(f'{txn_mgr.get_transaction_directory(txn_id)}/{dataframe_metadata.file_url}.v0_old')
        LoggingManager().log(f'Asserting before_image same as petastorm data', LoggingLevel.INFO)
        self.assertTrue(dataframes_equal(video_frames, before_image))

        after_image = read_file_from_image(f'{txn_mgr.get_transaction_directory(txn_id)}/{dataframe_metadata.file_url}.v0_new')
        LoggingManager().log(f'Asserting after_image same as updated video', LoggingLevel.INFO)
        self.assertTrue(dataframes_equal(updated_video_frames, after_image))

        updated_video_frames_petastorm = read_file_from_petastorm(self.storage_engine, dataframe_metadata)
        LoggingManager().log(f'Asserting updated video saved in storage engine', LoggingLevel.INFO)
        self.assertTrue(dataframes_equal(updated_video_frames, updated_video_frames_petastorm))

    @ignore_warnings
    def test_should_revert_changes_on_abort(self):
        dataframe_metadata = write_file(self.storage_engine, 'traffic001_6')
        initial_video_frames = read_file_from_petastorm(self.storage_engine, dataframe_metadata)

        txn_mgr = TransactionManager(storage_engine_passed=self.storage_engine)
        txn_id = txn_mgr.begin_transaction()

        update_operation = ObjectUpdateArguments('grayscale', 0, 300)
        txn_mgr.update_object(txn_id, dataframe_metadata, update_operation)
        after_image = read_file_from_image(f'{txn_mgr.get_transaction_directory(txn_id)}/{dataframe_metadata.file_url}.v0_new')

        txn_mgr.abort_transaction(txn_id)

        updated_video_frames = read_file_from_petastorm(self.storage_engine, dataframe_metadata)
        self.assertTrue(dataframes_equal(initial_video_frames, updated_video_frames))
        self.assertFalse(dataframes_equal(updated_video_frames, after_image))


if __name__ == '__main__':
    unittest.main()
