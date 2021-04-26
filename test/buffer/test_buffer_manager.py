import unittest
import os
import cv2
import shutil
import glob

from src.transaction.object_update_arguments import ObjectUpdateArguments
from test.utils.util_functions import ignore_warnings, \
                                        write_file, \
                                        read_file_from_fs, \
                                        apply_update_to_dataframe, \
                                        apply_update_to_dataframe_delta, \
                                        read_file_from_image, \
                                        read_file_from_petastorm, \
                                        dataframes_equal, \
                                        clear_petastorm_storage_folder, \
                                        write_dataframe_to_video
from src.storage.partitioned_petastorm_storage_engine import PartitionedPetastormStorageEngine
from src.models.storage.batch import Batch
from src.buffer.buffer_manager import BufferManager
from src.utils.logging_manager import LoggingLevel, LoggingManager
from src.config.constants import PETASTORM_STORAGE_FOLDER, \
                                 INPUT_VIDEO_FOLDER
                                 

class BufferManagerTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.storage_engine = PartitionedPetastormStorageEngine()
        LoggingManager().setEffectiveLevel(LoggingLevel.DEBUG)
    
    def tearDown(self):
        clear_petastorm_storage_folder()

    @ignore_warnings
    def test_should_read_group_into_slot(self):
        input_file_name = 'traffic001_6'
        dataframe_metadata = write_file(self.storage_engine, input_file_name)
        buffer_manager = BufferManager(10, self.storage_engine)

        buffer_manager.read_slot(dataframe_metadata, 0)
        self.assertIsNotNone(buffer_manager._slots[0])
        self.assertEqual(buffer_manager._slots[0].rows.get_group_num(), 0)
        buffer_manager.read_slot(dataframe_metadata, 0)
        self.assertIsNone(buffer_manager._slots[1])

        buffer_manager.read_slot(dataframe_metadata, 1)
        self.assertIsNotNone(buffer_manager._slots[1])
        self.assertEqual(buffer_manager._slots[1].rows.get_group_num(), 1)

    @ignore_warnings
    def test_should_write_group_into_slot(self):
        input_file_name = 'traffic001_6'
        dataframe_metadata = write_file(self.storage_engine, input_file_name)
        buffer_manager = BufferManager(10, self.storage_engine)

        before_batch = buffer_manager.read_slot(dataframe_metadata, 0)
        
        update_operation = ObjectUpdateArguments('invert_color', 0, 25)
        new_batch_delta = Batch(apply_update_to_dataframe_delta(before_batch.frames, update_operation))
        
        expected_new_batch = Batch(frames=apply_update_to_dataframe(before_batch.frames, update_operation))
        
        buffer_manager.write_slot(dataframe_metadata, new_batch_delta)
        self.assertIsNotNone(buffer_manager._slots[0])
        actual_new_batch = buffer_manager.read_slot(dataframe_metadata, 0)

        self.assertTrue(dataframes_equal(actual_new_batch.frames, expected_new_batch.frames))

    @ignore_warnings
    def test_should_flush_slot(self):
        input_file_name = 'traffic001_6'
        dataframe_metadata = write_file(self.storage_engine, input_file_name)
        buffer_manager = BufferManager(10, self.storage_engine)

        before_batch = buffer_manager.read_slot(dataframe_metadata, 0)
        
        update_operation = ObjectUpdateArguments('invert_color', 0, 25)
        new_batch_delta = Batch(apply_update_to_dataframe_delta(before_batch.frames, update_operation))
        
        expected_new_batch = Batch(frames=apply_update_to_dataframe(before_batch.frames, update_operation))
        
        buffer_manager.write_slot(dataframe_metadata, new_batch_delta)
        buffer_manager.flush_all_slots()
        buffer_manager.discard_all_slots()

        self.assertIsNone(buffer_manager._slots[0])

        after_batch = buffer_manager.read_slot(dataframe_metadata, 0)

        self.assertTrue(dataframes_equal(after_batch.frames, expected_new_batch.frames))

    @ignore_warnings
    def test_should_discard_slot(self):
        input_file_name = 'traffic001_6'
        dataframe_metadata = write_file(self.storage_engine, input_file_name)
        buffer_manager = BufferManager(10, self.storage_engine)

        before_batch = buffer_manager.read_slot(dataframe_metadata, 0)
        before_batch_copy = Batch(frames=before_batch.frames.copy(deep=True))
        
        update_operation = ObjectUpdateArguments('invert_color', 0, 25)
        new_batch_delta = Batch(apply_update_to_dataframe_delta(before_batch.frames, update_operation))
        
        expected_new_batch = Batch(frames=apply_update_to_dataframe(before_batch.frames, update_operation))
        
        buffer_manager.write_slot(dataframe_metadata, new_batch_delta)
        buffer_manager.discard_all_slots()

        self.assertIsNone(buffer_manager._slots[0])

        after_batch = buffer_manager.read_slot(dataframe_metadata, 0)

        self.assertTrue(dataframes_equal(after_batch.frames, before_batch_copy.frames))

    @ignore_warnings
    def test_should_use_lru_eviction_policy(self):
        input_file_name = 'traffic001_6'
        dataframe_metadata = write_file(self.storage_engine, input_file_name)
        buffer_manager = BufferManager(2, self.storage_engine)

        buffer_manager.read_slot(dataframe_metadata, 0)
        self.assertEqual(buffer_manager._lru, [0])
        self.assertEqual(buffer_manager._get_slot(dataframe_metadata, 0)[1], 0)
        buffer_manager.read_slot(dataframe_metadata, 1)
        self.assertEqual(buffer_manager._lru, [0, 1])
        self.assertEqual(buffer_manager._get_slot(dataframe_metadata, 1)[1], 1)

        buffer_manager.read_slot(dataframe_metadata, 2)
        self.assertEqual(buffer_manager._lru, [1, 0])
        self.assertIsNone(buffer_manager._get_slot(dataframe_metadata, 0)[1])
        self.assertEqual(buffer_manager._get_slot(dataframe_metadata, 2)[1], 0)
        buffer_manager.read_slot(dataframe_metadata, 1)
        self.assertEqual(buffer_manager._lru, [0, 1])

if __name__ == '__main__':
    unittest.main()     