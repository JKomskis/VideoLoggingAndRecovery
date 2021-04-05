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
        self.expected_log = bytes()

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

    def assert_appended_to_log(self, expected_appended):
        self.log_mgr.flush()

        with open(self.log_file_path, 'rb') as log_file:
            actual = log_file.read()
            self.expected_log += bytes(expected_appended)
            self.assertEqual(len(self.expected_log), len(actual))
            for i in range(len(actual)):
                self.assertEqual(self.expected_log[i], actual[i], f'first differed at byte {i}')

    def test_begin_record(self):
        self.log_mgr.log_begin_txn_record(257)
        self.assert_appended_to_log([
            13, 0, 0, 0,
            2,
            1, 1, 0, 0,
            0, 0, 0, 0
        ])
        self.assertEqual({257: 0}, self.log_mgr.last_lsn)
        self.log_mgr.log_begin_txn_record(259)
        self.assert_appended_to_log([
            13, 0, 0, 0,
            2,
            3, 1, 0, 0,
            0, 0, 0, 0
        ])
        self.assertEqual({257: 0, 259: 13}, self.log_mgr.last_lsn)

    def test_update_record(self):
        self.log_mgr.log_begin_txn_record(1)
        self.assert_appended_to_log([
            13, 0, 0, 0,
            2,
            1, 0, 0, 0,
            0, 0, 0, 0
        ])
        self.assertEqual({1: 0}, self.log_mgr.last_lsn)

        self.log_mgr.log_update_record(1, "traffic001.mp4", "txn_storage/1/0", "txn_storage/1/1")
        self.assert_appended_to_log([
            69, 0, 0, 0,
            3,
            1, 0, 0, 0,
            0, 0, 0, 0,
            14, 0, 0, 0,
            116,114,97,102,102,105,99,48,48,49,46,109,112,52,
            15, 0, 0, 0,
            116,120,110,95,115,116,111,114,97,103,101,47,49,47,48,
            15, 0, 0, 0,
            116,120,110,95,115,116,111,114,97,103,101,47,49,47,49
        ])
        self.assertEqual({1: 13}, self.log_mgr.last_lsn)

        self.log_mgr.log_begin_txn_record(2)
        self.assert_appended_to_log([
            13, 0, 0, 0,
            2,
            2, 0, 0, 0,
            0, 0, 0, 0
        ])
        self.assertEqual({1: 13, 2: 82}, self.log_mgr.last_lsn)

        self.log_mgr.log_update_record(1, "traffic001.mp4", "txn_storage/1/1", "txn_storage/1/2")
        self.assert_appended_to_log([
            69, 0, 0, 0,
            3,
            1, 0, 0, 0,
            13, 0, 0, 0,
            14, 0, 0, 0,
            116,114,97,102,102,105,99,48,48,49,46,109,112,52,
            15, 0, 0, 0,
            116,120,110,95,115,116,111,114,97,103,101,47,49,47,49,
            15, 0, 0, 0,
            116,120,110,95,115,116,111,114,97,103,101,47,49,47,50
        ])
        self.assertEqual({1: 95, 2: 82}, self.log_mgr.last_lsn)

        self.log_mgr.log_update_record(2, "traffic002.mp4", "txn_storage/2/0", "txn_storage/2/1")
        self.assert_appended_to_log([
            69, 0, 0, 0,
            3,
            2, 0, 0, 0,
            82, 0, 0, 0,
            14, 0, 0, 0,
            116,114,97,102,102,105,99,48,48,50,46,109,112,52,
            15, 0, 0, 0,
            116,120,110,95,115,116,111,114,97,103,101,47,50,47,48,
            15, 0, 0, 0,
            116,120,110,95,115,116,111,114,97,103,101,47,50,47,49
        ])
        self.assertEqual({1: 95, 2: 164}, self.log_mgr.last_lsn)

    def test_commit_record(self):
        self.log_mgr.log_begin_txn_record(1)
        self.assert_appended_to_log([
            13, 0, 0, 0,
            2,
            1, 0, 0, 0,
            0, 0, 0, 0
        ])
        self.assertEqual({1: 0}, self.log_mgr.last_lsn)

        self.log_mgr.log_commit_txn_record(1)
        self.assert_appended_to_log([
            13, 0, 0, 0,
            4,
            1, 0, 0, 0,
            0, 0, 0, 0
        ])
        self.assertEqual({}, self.log_mgr.last_lsn)

        self.log_mgr.log_begin_txn_record(2)
        self.assert_appended_to_log([
            13, 0, 0, 0,
            2,
            2, 0, 0, 0,
            0, 0, 0, 0
        ])
        self.assertEqual({2: 26}, self.log_mgr.last_lsn)
        self.log_mgr.log_update_record(2, "traffic002.mp4", "txn_storage/2/0", "txn_storage/2/1")
        self.assert_appended_to_log([
            69, 0, 0, 0,
            3,
            2, 0, 0, 0,
            26, 0, 0, 0,
            14, 0, 0, 0,
            116,114,97,102,102,105,99,48,48,50,46,109,112,52,
            15, 0, 0, 0,
            116,120,110,95,115,116,111,114,97,103,101,47,50,47,48,
            15, 0, 0, 0,
            116,120,110,95,115,116,111,114,97,103,101,47,50,47,49
        ])
        self.assertEqual({2: 39}, self.log_mgr.last_lsn)
        self.log_mgr.log_begin_txn_record(3)
        self.assert_appended_to_log([
            13, 0, 0, 0,
            2,
            3, 0, 0, 0,
            0, 0, 0, 0
        ])
        self.assertEqual({2: 39, 3: 108}, self.log_mgr.last_lsn)
        self.log_mgr.log_commit_txn_record(2)
        self.assert_appended_to_log([
            13, 0, 0, 0,
            4,
            2, 0, 0, 0,
            39, 0, 0, 0
        ])
        self.assertEqual({3: 108}, self.log_mgr.last_lsn)

    def test_rollback_txn(self):
        self.log_mgr.log_begin_txn_record(4)
        self.log_mgr.log_update_record(4, "traffic001.mp4", "txn_storage/1/0", "txn_storage/1/1")
        self.log_mgr.log_abort_txn_record(4)

if __name__ == '__main__':
    unittest.main()