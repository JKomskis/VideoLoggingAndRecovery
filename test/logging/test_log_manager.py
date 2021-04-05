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

    def setUp(self):
        super().setUp()
        self.log_file_path = f'{TRANSACTION_STORAGE_FOLDER}/transactions.log'
        self.temp_log_file_path = f'{self.log_file_path}.old'
        if os.path.exists(self.log_file_path):
            os.rename(self.log_file_path, self.temp_log_file_path)
        self.log_mgr = LogManager()

    def tearDown(self):
        os.rename(self.temp_log_file_path, self.log_file_path)
        super().tearDown()

    # def tearDown(self):
    #     shutil.rmtree(TRANSACTION_STORAGE_FOLDER, ignore_errors=True)
    #     for filename in os.listdir(PETASTORM_STORAGE_FOLDER):
    #         file_path = os.path.join(PETASTORM_STORAGE_FOLDER, filename)
    #         if os.path.isfile(file_path) or os.path.islink(file_path):
    #             os.unlink(file_path)
    #         elif os.path.isdir(file_path):
    #             shutil.rmtree(file_path)

    def assert_log_contents(self, expected):
        self.log_mgr.flush()

        with open(self.log_file_path, 'rb') as log_file:
            actual = log_file.read()
            self.assertEqual(expected, actual)

    def test_begin_record(self):
        self.log_mgr.log_begin_txn_record(257)
        self.assert_log_contents([
            13, 0, 0, 0,
            2,
            1, 1, 0, 0,
            0, 0, 0, 0
        ])

    # def test_update_record(self):
    #     log_file_path = f'{TRANSACTION_STORAGE_FOLDER}/transactions.log'
    #     temp_log_file_path = f'{log_file_path}.old'
    #     if os.path.exists(log_file_path):
    #         os.rename(log_file_path, temp_log_file_path)

    #     try:
    #         log_mgr = LogManager()
    #         log_mgr.log_begin_txn_record(257)

    #         del log_mgr # to force the file to be flushed to disk
    #         with open(log_file_path, 'rb') as log_file:
    #             log_contents = log_file.read()
    #             self.assertEqual(log_contents, bytearray([
    #                 13, 0, 0, 0,
    #                 2,
    #                 1, 1, 0, 0,
    #                 0, 0, 0, 0
    #             ]))
    #     finally:
    #         os.rename(temp_log_file_path, log_file_path)


if __name__ == '__main__':
    unittest.main()