import os
from enum import Enum

from src.config.constants import TRANSACTION_STORAGE_FOLDER
from src.utils.logging_manager import LoggingLevel, LoggingManager

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

    def flush(self):
        self.log_file.flush()

    # structure common to all log records:
    #  length  record_type  txn_id  prev_lsn  [fields]
    #  int32                 int32    int32
    # where each field of fields is serialized as
    #  length   data
    #   int32
    def _write_log_record(self, record_type: LogRecordType, txn_id: int, fields: [bytes] = []) -> None:
        record_type = record_type.value.to_bytes(1, byteorder='little')
        txn_id = txn_id.to_bytes(4, byteorder='little')
        last_lsn = (
            self.last_lsn[txn_id] if txn_id in self.last_lsn else 0
        ).to_bytes(4, byteorder='little')

        record = record_type + txn_id + last_lsn
        for field in fields:
            record += len(field).to_bytes(4, byteorder='little')
            record += field
        self.log_file.write((len(record) + 4).to_bytes(4, byteorder='little') + record)

    # each log record should include txn_id, offset of last log record for this txn,
    # type of record, and length of record so we can quickly seek over it
    def log_begin_txn_record(self, txn_id: int) -> None:
        LoggingManager().log(f'Begin txn {txn_id}', LoggingLevel.DEBUG)
        self.last_lsn[txn_id] = self.log_file.tell()
        self._write_log_record(LogRecordType.BEGIN, txn_id)

    def log_update_record(self, txn_id: int, name: str, before_path: str, after_path: str) -> None:
        LoggingManager().log(f'Update, txn {txn_id} name {str} from {before_path} to {after_path}', LoggingLevel.DEBUG)
        self.last_lsn[txn_id] = self.log_file.tell()
        self._write_log_record(LogRecordType.UPDATE, txn_id, [
            name, before_path, after_path
        ])
        # write log record that includes txn id, name of video updated, path to before image, and path to after image

    def log_commit_txn_record(self, txn_id: int) -> None:
        LoggingManager().log(f'Commit txn {txn_id}', LoggingLevel.DEBUG)
        self._write_log_record(LogRecordType.COMMIT, txn_id)
        self.log_file.flush()
        del self.last_lsn[txn_id]

    def log_abort_txn_record(self, txn_id: int) -> None:
        LoggingManager().log(f'Abort txn {txn_id}', LoggingLevel.DEBUG)
        self._write_log_record(LogRecordType.ABORT, txn_id)
        self.rollback_txn(txn_id)
        del self.last_lsn[txn_id]

    def rollback_txn(self, txn_id: int) -> None:
        LoggingManager().log(f'Rollback txn {txn_id}', LoggingLevel.DEBUG)
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