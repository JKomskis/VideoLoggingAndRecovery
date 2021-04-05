import os
from enum import Enum

from src.config.constants import TRANSACTION_STORAGE_FOLDER

class LogRecordType(Enum):
    UNKNOWN = 1
    BEGIN = 2
    UPDATE = 3
    COMMIT = 4
    ABORT = 5

class LogManager():
    def __init__(self, log_file_name='transactions.log'):
        self.log_file_path = f'{TRANSACTION_STORAGE_FOLDER}/{log_file_name}'
        # setup log file if needed
        if not os.path.isdir(TRANSACTION_STORAGE_FOLDER):
            os.mkdir(TRANSACTION_STORAGE_FOLDER)

        if not os.path.isfile(self.log_file_path):
            # create file if doesn't exist (opening with mode rb+ will error)
            with open(self.log_file_path, "w") as _:
                pass

        self.last_lsn = {}

        self.log_file = open(self.log_file_path, "rb+")
        # Seek to end of log
        self.log_file.seek(0, 2)
        self.offset = self.log_file.tell()

    def __del__(self):
        self.log_file.close()

    # each log record should include txn_id, offset of last log record for this txn,
    # type of record, and length of record so we can quickly seek over it
    def log_begin_txn_record(self, txn_id: int) -> None:
        record_type = LogRecordType.BEGIN.value.to_bytes(1, byteorder='little')
        txn_id = txn_id.to_bytes(4, byteorder='little')

        self.last_lsn[txn_id] = self.log_file.tell()

        record = bytes(record_type) + bytes(txn_id)
        self.log_file.write(record)
        pass

    def log_update_record(self, txn_id: int, name: str, before_path: str, after_path: str) -> None:
        # write log record that includes txn id, name of video updated, path to before image, and path to after image
        # format:
        # type  txn_id  lengths  name  before_path  after_path
        # where lengths is (len(name), len(before_path), len(after_path))
        record_type = LogRecordType.UPDATE.value.to_bytes(1, byteorder='little')
        txn_id = txn_id.to_bytes(4, byteorder='little')
        name = name.encode('utf8')
        name_len = len(name).to_bytes(4, byteorder='little')
        before_path = before_path.encode('utf8')
        before_path_len = len(before_path).to_bytes(4, byteorder='little')
        after_path = after_path.encode('utf8')
        after_path_len = len(after_path).to_bytes(4, byteorder='little')

        self.last_lsn[txn_id] = self.log_file.tell()

        record = bytes(record_type) + bytes(txn_id) + bytes(name_len) + bytes(before_path_len) + bytes(after_path_len) + name + before_path + after_path
        self.log_file.write(record)
        pass

    def log_commit_txn_record(self, txn_id: int) -> None:
        record_type = LogRecordType.COMMIT.value.to_bytes(1, byteorder='little')
        txn_id = txn_id.to_bytes(4, byteorder='little')

        record = bytes(record_type) + bytes(txn_id)
        self.log_file.write(record)

        del self.last_lsn[txn_id]
        pass

    def log_abort_txn_record(self, txn_id: int) -> None:
        record_type = LogRecordType.ABORT.value.to_bytes(1, byteorder='little')
        txn_id = txn_id.to_bytes(4, byteorder='little')

        record = bytes(record_type) + bytes(txn_id)
        self.log_file.write(record)

        self.rollback_txn(txn_id)
        del self.last_lsn[txn_id]
        pass

    def rollback_txn(self, txn_id: int) -> None:
        # read log file and undo txn's changes
        # May be able to use write_serialized_image in transaction_manger for doing this
        pass

    def recover_log(self) -> None:
        # ARIES style three phase recovery protocol
        # Analysis
        # Scan through each log record and add txn_id to self.last_lsn
        # If commit or abort record found remove from self.last_lsn
        # Commit or abort only written after all writing/rolling back done,
        # so nothing to do for these transactions

        # Redo
        # This phase may not actually be needed, since updates immediately write
        # their changes to the storage engine

        # Undo
        # For every transaction in self.last_lsn, rollback the transaction
        # Once each transaction is done, write an abort record to the log
        # and delete their folder in the transaction_storage folder
        # Once all rollbacks done, clear the last_lsn table
        pass