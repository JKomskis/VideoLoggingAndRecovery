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
                                 

class PartitionedPetastormStorageEngineTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.storage_engine = PartitionedPetastormStorageEngine()
        LoggingManager().setEffectiveLevel(LoggingLevel.DEBUG)
    
    def tearDown(self):
        clear_petastorm_storage_folder()

    @ignore_warnings
    def test_should_create_separate_datasets(self):
        input_file_name = 'traffic001_6'
        dataframe_metadata = write_file(self.storage_engine, input_file_name)
        self.assertEqual(len(glob.glob(f'{PETASTORM_STORAGE_FOLDER}/{INPUT_VIDEO_FOLDER}/{input_file_name}.mp4/group*')),
                         4)

if __name__ == '__main__':
    unittest.main()     