import os
from enum import Enum
import pickle

from src.transaction.object_update_arguments import ObjectUpdateArguments
from src.transaction.util import apply_object_update_arguments_to_buffer_manager
from src.config.constants import TRANSACTION_STORAGE_FOLDER
from src.utils.logging_manager import LoggingLevel, LoggingManager
from src.transaction.opencv_update_processor import OpenCVUpdateProcessor
from src.catalog.models.df_metadata import DataFrameMetadata
from src.pressure_point.pressure_point_manager import PressurePointManager
from src.pressure_point.pressure_point import PressurePoint, PressurePointLocation, PressurePointBehavior

class LogRecordType(Enum):
    UNKNOWN = 1
    BEGIN = 2
    LOGICAL_UPDATE = 3
    PHYSICAL_UPDATE = 4
    COMMIT = 5
    ABORT = 6
    TXNEND = 7
    LOGICAL_CLR = 8
    PHYSICAL_CLR = 9

class LogicalLogManager():
    def __init__(self, buffer_manager, log_file_name='transactions.log'):
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

        self.update_processor = OpenCVUpdateProcessor()
        self.buffer_manager = buffer_manager

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
    def _write_log_record(self, record_type: LogRecordType, txn_id: int, fields: [bytes] = []) -> int:
        record_type = record_type.value.to_bytes(1, byteorder='little')
        txn_id_bytes = txn_id.to_bytes(4, byteorder='little')

        last_lsn = (
            self.last_lsn[txn_id] if txn_id in self.last_lsn else -1
        ).to_bytes(4, byteorder='little', signed=True)
        self.last_lsn[txn_id] = self.log_file.tell()

        record = record_type + txn_id_bytes + last_lsn
        for field in fields:
            record += len(field).to_bytes(4, byteorder='little')
            record += field
        self.log_file.write((len(record) + 4).to_bytes(4, byteorder='little') + record)
        return self.last_lsn[txn_id]

    # each log record should include txn_id, offset of last log record for this txn,
    # type of record, and length of record so we can quickly seek over it
    def log_begin_txn_record(self, txn_id: int) -> None:
        LoggingManager().log(f'Begin txn {txn_id}', LoggingLevel.DEBUG)
        self._write_log_record(LogRecordType.BEGIN, txn_id)

    def log_logical_update_record(self, txn_id: int, dataframe_metadata: DataFrameMetadata, update_arguments: ObjectUpdateArguments) -> int:
        # write log record that includes txn id, dataframe_metadata, and update operation
        LoggingManager().log(f'Update, txn {txn_id} name {dataframe_metadata.file_url} using {update_arguments}', LoggingLevel.DEBUG)
        return self._write_log_record(LogRecordType.LOGICAL_UPDATE, txn_id, [
            dataframe_metadata.serialize(), update_arguments.serialize()
        ])

    def log_commit_txn_record(self, txn_id: int) -> None:
        LoggingManager().log(f'Commit txn {txn_id}', LoggingLevel.DEBUG)
        self.log_file.flush()
        self._write_log_record(LogRecordType.COMMIT, txn_id)
        del self.last_lsn[txn_id]

    def log_abort_txn_record(self, txn_id: int) -> None:
        LoggingManager().log(f'Abort txn {txn_id}', LoggingLevel.DEBUG)
        self._write_log_record(LogRecordType.ABORT, txn_id)
    
    def log_logical_clr_record(self, txn_id: int, dataframe_metadata: DataFrameMetadata, update_arguments: ObjectUpdateArguments, undo_next_lsn: int) -> int:
        # write log record that includes txn_id, dataframe_metadata, reversed update operation, and undo next lsn
        LoggingManager().log(f'CLR, txn {txn_id} name {dataframe_metadata.file_url} using {update_arguments}, undo_next_lsn {undo_next_lsn}', LoggingLevel.DEBUG)
        return self._write_log_record(LogRecordType.LOGICAL_CLR, txn_id, [
            dataframe_metadata.serialize(),
            update_arguments.serialize(),
            undo_next_lsn.to_bytes(4, byteorder='little', signed=True)
        ])

    def rollback_txn(self, txn_id: int) -> None:
        LoggingManager().log(f'Rollback txn {txn_id}', LoggingLevel.DEBUG)
        # read log file and undo txn's changes
        original_seek_offset = self.log_file.tell()
        lsn = self.last_lsn[txn_id]
        while lsn != -1:
            self.log_file.seek(lsn)
            entry_len = int.from_bytes(self.log_file.read(4), byteorder='little')
            rest_of_entry = self.log_file.read(entry_len - 4)
            # Seek back to end of file in case we need to write a CLR
            self.log_file.seek(0, 2)

            record_type, read_txn_id, prev_lsn = self.parse_record_header(rest_of_entry)
            lsn = prev_lsn
            # Undo logical update
            if record_type == LogRecordType.LOGICAL_UPDATE:
                dataframe_metadata, update_arguments = self.parse_logical_update_record(rest_of_entry)
                reversed_update_arguments = self.update_processor.reverse(update_arguments)
                # log CLR to log file
                # undo next lsn is the prev_lsn of the current log record
                clr_lsn = self.log_logical_clr_record(txn_id,
                                                    dataframe_metadata,
                                                    reversed_update_arguments,
                                                    lsn)

                if PressurePointManager().has_pressure_point(PressurePoint(
                    PressurePointLocation.LOGICAL_LOG_MANAGER_ROLLBACK_AFTER_CLR,
                    PressurePointBehavior.EARLY_RETURN)):
                        # Results in a log with a CLR record for testing purposes
                        return

                # Revert the update
                LoggingManager().log(f'Reverting txn_id {read_txn_id} file_url {dataframe_metadata.file_url} using {reversed_update_arguments}', LoggingLevel.DEBUG)
                apply_object_update_arguments_to_buffer_manager(self.buffer_manager,
                                                                self.update_processor,
                                                                dataframe_metadata,
                                                                reversed_update_arguments,
                                                                clr_lsn)
            # Undo physical update
            elif record_type == LogRecordType.PHYSICAL_UPDATE:
                pass
            # Don't undo logical clr, but set lsn to its undo_next_lsn value
            elif record_type == LogRecordType.LOGICAL_CLR:
                dataframe_metadata, update_arguments, undo_next_lsn = self.parse_logical_clr_record(rest_of_entry)
                LoggingManager().log(f'Found logical CLR, setting lsn to {undo_next_lsn}', LoggingLevel.DEBUG)
                lsn = undo_next_lsn
            # Don't undo physical clr, but set lsn to its undo_next_lsn value
            elif record_type == LogRecordType.PHYSICAL_CLR:
                pass

        self.log_file.seek(original_seek_offset)
        del self.last_lsn[txn_id]
        # May be able to use write_serialized_image in transaction_manger for doing this

    # Two phase recovery protocol
    # 1. Analysis
    # Scan through each log record and add txn_id to self.last_lsn
    # If commit or txnend record found remove from self.last_lsn
    # Commit or txnend only written after all writing/rolling back done,
    # so nothing to do for these transactions
    # 2. Redo
    # Scan through each log record and replay if lsn > max(lsn) from corresponding batch
    # 3. Undo
    # For every transaction in self.last_lsn, rollback the transaction
    # Once each transaction is done, write a txnend record to the log
    # and delete their folder in the transaction_storage folder
    # Once all rollbacks done, clear the last_lsn table
    #   I think it should be sufficient to just call abort_txn here?
    #   it does rollback, writes an abort record, and removes from LSN table
    def recover_log(self) -> None:
        # Analysis
        LoggingManager().log(f'Starting analysis phase', LoggingLevel.DEBUG)
        self.log_file.seek(0)
        offset = 0
        while True:
            len_bytes = self.log_file.read(4)
            if len(len_bytes) == 0:
                break
            entry_len = int.from_bytes(len_bytes, byteorder='little')
            rest_of_entry = self.log_file.read(entry_len - 4)

            record_type, record_txn_id, _ = self.parse_record_header(rest_of_entry)
            LoggingManager().log(f'Got type {record_type} txn_id {record_txn_id} at offset {offset}', LoggingLevel.DEBUG)

            self.last_lsn[record_txn_id] = offset
            if record_type == LogRecordType.COMMIT or record_type == LogRecordType.TXNEND:
                del self.last_lsn[record_txn_id]

            offset += entry_len
            assert offset == self.log_file.tell()
        LoggingManager().log(f'Txn active during crash: {self.last_lsn}', LoggingLevel.DEBUG)

        # Redo
        LoggingManager().log(f'Starting redo phase', LoggingLevel.DEBUG)
        self.log_file.seek(0)
        offset = 0
        while True:
            curr_lsn = offset
            len_bytes = self.log_file.read(4)
            if len(len_bytes) == 0:
                break
            entry_len = int.from_bytes(len_bytes, byteorder='little')
            rest_of_entry = self.log_file.read(entry_len - 4)

            record_type, record_txn_id, _ = self.parse_record_header(rest_of_entry)
            LoggingManager().log(f'Got type {record_type} txn_id {record_txn_id} at offset {offset}', LoggingLevel.DEBUG)
            # Redo logical update
            if record_type == LogRecordType.LOGICAL_UPDATE:
                dataframe_metadata, update_arguments = self.parse_logical_update_record(rest_of_entry)
                LoggingManager().log(f'Redoing logical update file_url {dataframe_metadata.file_url} using {update_arguments}', LoggingLevel.DEBUG)
                apply_object_update_arguments_to_buffer_manager(self.buffer_manager,
                                                                self.update_processor,
                                                                dataframe_metadata,
                                                                update_arguments,
                                                                curr_lsn)
            # Redo physical update
            elif record_type == LogRecordType.PHYSICAL_UPDATE:
                pass
            # Redo logical CLR
            elif record_type == LogRecordType.LOGICAL_CLR:
                dataframe_metadata, update_arguments, _ = self.parse_logical_clr_record(rest_of_entry)
                LoggingManager().log(f'Redoing logical clr file_url {dataframe_metadata.file_url} using {update_arguments}', LoggingLevel.DEBUG)
                apply_object_update_arguments_to_buffer_manager(self.buffer_manager,
                                                                self.update_processor,
                                                                dataframe_metadata,
                                                                update_arguments,
                                                                curr_lsn)
            # Redo physical CLR
            elif record_type == LogRecordType.PHYSICAL_CLR:
                pass
            offset += entry_len
            assert offset == self.log_file.tell()

        # Undo
        # Since we're not worrying about concurrent transactions, we can rollback
        # transactions in the order of their last LSN
        LoggingManager().log(f'Starting undo phase', LoggingLevel.DEBUG)
        to_undo = list(self.last_lsn.items())
        to_undo.sort(key=lambda x: x[1], reverse=True)
        LoggingManager().log(f'TXNs to undo: {to_undo}', LoggingLevel.DEBUG)
        for txn_to_undo in to_undo:
            self.rollback_txn(txn_to_undo[0])
        
        self.log_file.seek(0)
        self.last_lsn.clear()

    def parse_record_header(self, rest_of_entry: bytes) -> (LogRecordType, int, int):
        record_type = LogRecordType(rest_of_entry[0])
        txn_id = int.from_bytes(rest_of_entry[1:5], byteorder='little')
        prev_lsn = int.from_bytes(rest_of_entry[5:9], byteorder='little', signed=True)
        
        return record_type, txn_id, prev_lsn

    def parse_logical_update_record(self, rest_of_entry: bytes) -> (DataFrameMetadata, ObjectUpdateArguments):
        df_metadata_len = int.from_bytes(rest_of_entry[9:13], byteorder='little')
        update_arguments_pos = 13 + df_metadata_len
        dataframe_metadata = DataFrameMetadata.deserialize(rest_of_entry[13:update_arguments_pos])
        
        update_arguments_len = int.from_bytes(rest_of_entry[update_arguments_pos:update_arguments_pos+4], byteorder='little')
        update_arguments_bytes = rest_of_entry[update_arguments_pos+4:]
        update_arguments = ObjectUpdateArguments.deserialize(update_arguments_bytes)

        return dataframe_metadata, update_arguments

    def parse_logical_clr_record(self, rest_of_entry: bytes) -> (DataFrameMetadata, ObjectUpdateArguments, int):
        df_metadata_len = int.from_bytes(rest_of_entry[9:13], byteorder='little')
        update_arguments_pos = 13 + df_metadata_len
        dataframe_metadata = DataFrameMetadata.deserialize(rest_of_entry[13:update_arguments_pos])
        
        update_arguments_len = int.from_bytes(rest_of_entry[update_arguments_pos:update_arguments_pos+4], byteorder='little')
        undo_next_lsn_pos = update_arguments_pos + 4 + update_arguments_len
        update_arguments = ObjectUpdateArguments.deserialize(rest_of_entry[update_arguments_pos+4:undo_next_lsn_pos])

        undo_next_lsn = int.from_bytes(rest_of_entry[undo_next_lsn_pos+4:], byteorder='little')

        return dataframe_metadata, update_arguments, undo_next_lsn