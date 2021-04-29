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
        txn_id_bytes = txn_id.to_bytes(4, byteorder='little')

        last_lsn = (
            self.last_lsn[txn_id] if txn_id in self.last_lsn else 0
        ).to_bytes(4, byteorder='little')
        self.last_lsn[txn_id] = self.log_file.tell()

        record = record_type + txn_id_bytes + last_lsn
        for field in fields:
            record += len(field).to_bytes(4, byteorder='little')
            record += field
        self.log_file.write((len(record) + 4).to_bytes(4, byteorder='little') + record)

    # each log record should include txn_id, offset of last log record for this txn,
    # type of record, and length of record so we can quickly seek over it
    def log_begin_txn_record(self, txn_id: int) -> None:
        LoggingManager().log(f'Begin txn {txn_id}', LoggingLevel.INFO)
        self._write_log_record(LogRecordType.BEGIN, txn_id)

    def log_update_record(self, txn_id: int, name: str, before_path: str, after_path: str) -> None:
        LoggingManager().log(f'Update, txn {txn_id} name {name} from {before_path} to {after_path}', LoggingLevel.INFO)
        self._write_log_record(LogRecordType.UPDATE, txn_id, [
            name.encode("utf8"), before_path.encode("utf8"), after_path.encode("utf8")
        ])
        # write log record that includes txn id, name of video updated, path to before image, and path to after image

    def log_commit_txn_record(self, txn_id: int) -> None:
        LoggingManager().log(f'Commit txn {txn_id}', LoggingLevel.INFO)
        self.log_file.flush()
        self._write_log_record(LogRecordType.COMMIT, txn_id)
        del self.last_lsn[txn_id]

    def log_abort_txn_record(self, txn_id: int) -> None:
        LoggingManager().log(f'Abort txn {txn_id}', LoggingLevel.INFO)
        self._write_log_record(LogRecordType.ABORT, txn_id)
        del self.last_lsn[txn_id]

    def rollback_txn(self, txn_id: int) -> [(str, str, str)]:
        rollbacks = []

        LoggingManager().log(f'Rollback txn {txn_id}', LoggingLevel.INFO)
        # read log file and undo txn's changes
        original_seek_offset = self.log_file.tell()
        lsn = self.last_lsn[txn_id]
        while lsn != 0:
            self.log_file.seek(lsn)
            entry_len = int.from_bytes(self.log_file.read(4), byteorder='little')
            rest_of_entry = self.log_file.read(entry_len - 4)

            record_type = LogRecordType(rest_of_entry[0])
            read_txn_id = int.from_bytes(rest_of_entry[1:5], byteorder='little')
            lsn = int.from_bytes(rest_of_entry[5:9], byteorder='little')
            if record_type == LogRecordType.UPDATE:
                name_len = int.from_bytes(rest_of_entry[9:13], byteorder='little')
                before_pos = 13 + name_len
                name = rest_of_entry[13:before_pos].decode('utf8')
                before_len = int.from_bytes(rest_of_entry[before_pos:before_pos+4], byteorder='little')
                after_pos = before_pos + 4 + before_len
                before_path = rest_of_entry[before_pos+4:after_pos].decode('utf8')
                after_len = int.from_bytes(rest_of_entry[after_pos:after_pos+4], byteorder='little')
                after_path = rest_of_entry[after_pos+4:after_pos+4+after_len].decode('utf8')

                LoggingManager().log(f'Change to revert: name {name} from {after_path} back to {before_path}', LoggingLevel.INFO)
                rollbacks.append((name, after_path, before_path))

        self.log_file.seek(original_seek_offset)

        return rollbacks
        # Assume caller knows how to do the rollback to avoid circular dependency
        # E.g. may be able to use write_serialized_image

    # Two phase recovery protocol
    # 1. Analysis
    # Scan through each log record and add txn_id to self.last_lsn
    # If commit or abort record found remove from self.last_lsn
    # Commit or abort only written after all writing/rolling back done,
    # so nothing to do for these transactions
    # 2. Undo
    # For every transaction in self.last_lsn, rollback the transaction
    # Once each transaction is done, write an abort record to the log
    # and delete their folder in the transaction_storage folder
    # Once all rollbacks done, clear the last_lsn table
    #   I think it should be sufficient to just call abort_txn here?
    #   it does rollback, writes an abort record, and removes from LSN table
    # Redo is not needed since updates immediately write their changes to the storage engine
    def recover_log(self) -> dict:
        self.log_file.seek(0)
        offset = 0
        while True:
            len_bytes = self.log_file.read(4)
            if len(len_bytes) == 0:
                break
            entry_len = int.from_bytes(len_bytes, byteorder='little')
            rest_of_entry = self.log_file.read(entry_len - 4)

            record_type = LogRecordType(rest_of_entry[0])
            read_txn_id = int.from_bytes(rest_of_entry[1:5], byteorder='little')
            LoggingManager().log(f'Got type {record_type} txn_id {read_txn_id} at offset {offset}', LoggingLevel.INFO)

            self.last_lsn[read_txn_id] = offset
            if record_type == LogRecordType.COMMIT or record_type == LogRecordType.ABORT:
                del self.last_lsn[read_txn_id]

            offset += entry_len
            assert offset == self.log_file.tell()

        LoggingManager().log(f'Txn active during crash: {self.last_lsn}', LoggingLevel.INFO)
        rollbacks = {}
        for txn_id in list(self.last_lsn.keys()):
            rollbacks[txn_id] = self.rollback_txn(txn_id)
        return rollbacks
