import unittest
import os
import shutil

from src.Logging.log_manager import LogManager
from src.utils.logging_manager import LoggingLevel, LoggingManager
from src.config.constants import TRANSACTION_STORAGE_FOLDER

class TransactionManagerTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        LoggingManager().setEffectiveLevel(LoggingLevel.DEBUG)
    
    # def tearDown(self):
    #     shutil.rmtree(TRANSACTION_STORAGE_FOLDER, ignore_errors=True)
    #     for filename in os.listdir(PETASTORM_STORAGE_FOLDER):
    #         file_path = os.path.join(PETASTORM_STORAGE_FOLDER, filename)
    #         if os.path.isfile(file_path) or os.path.islink(file_path):
    #             os.unlink(file_path)
    #         elif os.path.isdir(file_path):
    #             shutil.rmtree(file_path)

    def test_begin_record(self):
        log_mgr = LogManager()
        log_mgr.log_begin_txn_record(1)

        

if __name__ == '__main__':
    unittest.main()     