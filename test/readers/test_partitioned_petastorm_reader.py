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
                                        read_file_from_image, \
                                        read_file_from_petastorm, \
                                        dataframes_equal, \
                                        clear_petastorm_storage_folder
from src.storage.partitioned_petastorm_storage_engine import PartitionedPetastormStorageEngine
from src.utils.logging_manager import LoggingLevel, LoggingManager
from src.config.constants import PETASTORM_STORAGE_FOLDER, \
                                 INPUT_VIDEO_FOLDER
                                 

class PartitionedPetastormReaderTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.storage_engine = PartitionedPetastormStorageEngine()
        LoggingManager().setEffectiveLevel(LoggingLevel.DEBUG)
    
    def tearDown(self):
        clear_petastorm_storage_folder()

    @ignore_warnings
    def test_should_read_all_groups(self):
        input_file_name = 'traffic001_6'
        dataframe_metadata = write_file(self.storage_engine, input_file_name)
        
        df = read_file_from_petastorm(self.storage_engine, dataframe_metadata)
        self.assertEqual(df.shape[0], 180)

    @ignore_warnings
    def test_should_read_only_one_group(self):
        input_file_name = 'traffic001_6'
        dataframe_metadata = write_file(self.storage_engine, input_file_name)
        
        df = read_file_from_petastorm(self.storage_engine, dataframe_metadata, group_num=1)
        self.assertEqual(df.shape[0], 50)

        df = read_file_from_petastorm(self.storage_engine, dataframe_metadata, group_num=2)
        self.assertEqual(df.shape[0], 50)

if __name__ == '__main__':
    unittest.main()     