import unittest
import os
import cv2
import shutil
import pandas as pd

from pandas.testing import assert_frame_equal
from src.transaction.optimized_transaction_manager import OptimizedTransactionManager
from src.transaction.object_update_arguments import ObjectUpdateArguments
from test.utils.util_functions import ignore_warnings, \
                                        write_file, \
                                        read_file_from_fs, \
                                        apply_update_to_dataframe, \
                                        read_file_from_image, \
                                        read_file_from_petastorm, \
                                        dataframes_equal, \
                                        clear_petastorm_storage_folder, \
                                        clear_transaction_storage_folder
from src.storage.partitioned_petastorm_storage_engine import PartitionedPetastormStorageEngine
from src.Logging.logical_log_manager import LogicalLogManager
from src.buffer.buffer_manager import BufferManager
from src.utils.logging_manager import LoggingLevel, LoggingManager
from src.config.constants import TRANSACTION_STORAGE_FOLDER
from src.pressure_point.pressure_point_manager import PressurePointManager
from src.pressure_point.pressure_point import PressurePoint, PressurePointLocation, PressurePointBehavior

class OptimizedTransactionManagerTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.storage_engine = PartitionedPetastormStorageEngine()
        LoggingManager().setEffectiveLevel(LoggingLevel.DEBUG)
    
    def tearDown(self):
        PressurePointManager().reset()
        clear_petastorm_storage_folder()
        clear_transaction_storage_folder()

    def setUp(self):
        PressurePointManager().reset()
        clear_petastorm_storage_folder()
        clear_transaction_storage_folder()

    def test_should_create_transaction(self):
        buffer_mgr = BufferManager(200, self.storage_engine)
        log_mgr = LogicalLogManager(buffer_mgr)
        txn_mgr = OptimizedTransactionManager(storage_engine_passed=self.storage_engine,
                                    log_manager_passed=log_mgr,
                                    buffer_manager_passed=buffer_mgr)
        txn_id = txn_mgr.begin_transaction()
        self.assertEqual(txn_id, 1)

        transaction_directory_path = f'{TRANSACTION_STORAGE_FOLDER}/{txn_id}'
        self.assertTrue(os.path.isdir(transaction_directory_path))
    
    @ignore_warnings
    def test_should_update_video_in_buffer_manager(self):
        update_operation = ObjectUpdateArguments('invert_color', 0, 299)

        dataframe_metadata = write_file(self.storage_engine, 'traffic001_6', include_lsn=True)

        video_frames = read_file_from_petastorm(self.storage_engine, dataframe_metadata)
        expected_updated_video_frames = apply_update_to_dataframe(video_frames, update_operation)

        buffer_mgr = BufferManager(200, self.storage_engine)
        log_mgr = LogicalLogManager(buffer_mgr)
        txn_mgr = OptimizedTransactionManager(storage_engine_passed=self.storage_engine,
                                    log_manager_passed=log_mgr,
                                    buffer_manager_passed=buffer_mgr)
        txn_id = txn_mgr.begin_transaction()
        txn_mgr.update_object(txn_id, dataframe_metadata, update_operation)

        actual_updated_video_frames = pd.DataFrame()
        for i in range(4):
            batch = buffer_mgr.read_slot(dataframe_metadata, i)
            actual_updated_video_frames = actual_updated_video_frames.append(batch.frames, ignore_index=True)

        LoggingManager().log(f'Asserting buffer manager is updated', LoggingLevel.INFO)
        self.assertTrue(dataframes_equal(expected_updated_video_frames, actual_updated_video_frames))

    @ignore_warnings
    def test_should_rollback_transaction_on_abort(self):
        update_operations = [ObjectUpdateArguments('invert_color', 0, 99),
                            ObjectUpdateArguments('invert_color', 100, 199)
        ]

        dataframe_metadata = write_file(self.storage_engine, 'traffic001_6', include_lsn=True)

        video_frames = read_file_from_petastorm(self.storage_engine, dataframe_metadata)
        updated_video_frames = apply_update_to_dataframe(video_frames, update_operations[0])
        updated_video_frames = apply_update_to_dataframe(updated_video_frames, update_operations[1])

        buffer_mgr = BufferManager(200, self.storage_engine)
        log_mgr = LogicalLogManager(buffer_mgr)
        txn_mgr = OptimizedTransactionManager(storage_engine_passed=self.storage_engine,
                                    log_manager_passed=log_mgr,
                                    buffer_manager_passed=buffer_mgr)
        txn_id = txn_mgr.begin_transaction()
        txn_mgr.update_object(txn_id, dataframe_metadata, update_operations[0])
        txn_mgr.update_object(txn_id, dataframe_metadata, update_operations[1])
        txn_mgr.abort_transaction(txn_id)

        actual_updated_video_frames = pd.DataFrame()
        for i in range(4):
            batch = buffer_mgr.read_slot(dataframe_metadata, i)
            actual_updated_video_frames = actual_updated_video_frames.append(batch.frames, ignore_index=True)

        LoggingManager().log(f'Asserting buffer manager is updated', LoggingLevel.INFO)
        self.assertTrue(dataframes_equal(video_frames, actual_updated_video_frames))
        self.assertFalse(dataframes_equal(updated_video_frames, actual_updated_video_frames))
    
    @ignore_warnings
    def test_recovery_should_redo_committed_transactions(self):
        update_operations = [ObjectUpdateArguments('invert_color', 0, 99),
                            ObjectUpdateArguments('invert_color', 100, 199)
        ]

        dataframe_metadata = write_file(self.storage_engine, 'traffic001_6', include_lsn=True)

        video_frames = read_file_from_petastorm(self.storage_engine, dataframe_metadata)
        updated_video_frames = apply_update_to_dataframe(video_frames, update_operations[0])
        updated_video_frames = apply_update_to_dataframe(updated_video_frames, update_operations[1])

        buffer_mgr = BufferManager(200, self.storage_engine)
        log_mgr = LogicalLogManager(buffer_mgr)
        txn_mgr = OptimizedTransactionManager(storage_engine_passed=self.storage_engine,
                                    log_manager_passed=log_mgr,
                                    buffer_manager_passed=buffer_mgr)
        txn_id = txn_mgr.begin_transaction()
        txn_mgr.update_object(txn_id, dataframe_metadata, update_operations[0])
        txn_mgr.update_object(txn_id, dataframe_metadata, update_operations[1])
        txn_mgr.commit_transaction(txn_id)

        # Reset buffer manager
        buffer_mgr.discard_all_slots()
        # Recovery from log
        log_mgr.recover_log()

        # Check contents of buffer manager after recovery
        actual_updated_video_frames = pd.DataFrame()
        for i in range(4):
            batch = buffer_mgr.read_slot(dataframe_metadata, i)
            actual_updated_video_frames = actual_updated_video_frames.append(batch.frames, ignore_index=True)

        LoggingManager().log(f'Asserting buffer manager is updated', LoggingLevel.INFO)
        self.assertTrue(dataframes_equal(updated_video_frames, actual_updated_video_frames))
        self.assertFalse(dataframes_equal(video_frames, actual_updated_video_frames))

    @ignore_warnings
    def test_recovery_should_redo_committed_transactions_partial_flush_1(self):
        update_operations = [ObjectUpdateArguments('invert_color', 0, 99),
                            ObjectUpdateArguments('invert_color', 100, 199)
        ]

        dataframe_metadata = write_file(self.storage_engine, 'traffic001_6', include_lsn=True)

        video_frames = read_file_from_petastorm(self.storage_engine, dataframe_metadata)
        updated_video_frames = apply_update_to_dataframe(video_frames, update_operations[0])
        updated_video_frames = apply_update_to_dataframe(updated_video_frames, update_operations[1])

        buffer_mgr = BufferManager(200, self.storage_engine)
        log_mgr = LogicalLogManager(buffer_mgr)
        txn_mgr = OptimizedTransactionManager(storage_engine_passed=self.storage_engine,
                                    log_manager_passed=log_mgr,
                                    buffer_manager_passed=buffer_mgr)
        txn_id = txn_mgr.begin_transaction()
        txn_mgr.update_object(txn_id, dataframe_metadata, update_operations[0])
        txn_mgr.update_object(txn_id, dataframe_metadata, update_operations[1])
        txn_mgr.commit_transaction(txn_id)

        # Reset buffer manager
        buffer_mgr.flush_slot(0)
        buffer_mgr.discard_all_slots()
        # Recovery from log
        log_mgr.recover_log()

        # Check contents of buffer manager after recovery
        actual_updated_video_frames = pd.DataFrame()
        for i in range(4):
            batch = buffer_mgr.read_slot(dataframe_metadata, i)
            actual_updated_video_frames = actual_updated_video_frames.append(batch.frames, ignore_index=True)

        LoggingManager().log(f'Asserting buffer manager is updated', LoggingLevel.INFO)
        self.assertTrue(dataframes_equal(updated_video_frames, actual_updated_video_frames))
        self.assertFalse(dataframes_equal(video_frames, actual_updated_video_frames))

    @ignore_warnings
    def test_recovery_should_redo_committed_transactions_partial_flush_2(self):
        update_operations = [ObjectUpdateArguments('invert_color', 0, 99),
                            ObjectUpdateArguments('invert_color', 100, 199)
        ]

        dataframe_metadata = write_file(self.storage_engine, 'traffic001_6', include_lsn=True)

        video_frames = read_file_from_petastorm(self.storage_engine, dataframe_metadata)
        updated_video_frames = apply_update_to_dataframe(video_frames, update_operations[0])
        updated_video_frames = apply_update_to_dataframe(updated_video_frames, update_operations[1])

        buffer_mgr = BufferManager(200, self.storage_engine)
        log_mgr = LogicalLogManager(buffer_mgr)
        txn_mgr = OptimizedTransactionManager(storage_engine_passed=self.storage_engine,
                                    log_manager_passed=log_mgr,
                                    buffer_manager_passed=buffer_mgr)
        txn_id = txn_mgr.begin_transaction()
        txn_mgr.update_object(txn_id, dataframe_metadata, update_operations[0])
        txn_mgr.update_object(txn_id, dataframe_metadata, update_operations[1])
        txn_mgr.commit_transaction(txn_id)

        # Reset buffer manager
        buffer_mgr.flush_slot(1)
        buffer_mgr.discard_all_slots()
        # Recovery from log
        log_mgr.recover_log()

        # Check contents of buffer manager after recovery
        actual_updated_video_frames = pd.DataFrame()
        for i in range(4):
            batch = buffer_mgr.read_slot(dataframe_metadata, i)
            actual_updated_video_frames = actual_updated_video_frames.append(batch.frames, ignore_index=True)

        LoggingManager().log(f'Asserting buffer manager is updated', LoggingLevel.INFO)
        self.assertTrue(dataframes_equal(updated_video_frames, actual_updated_video_frames))
        self.assertFalse(dataframes_equal(video_frames, actual_updated_video_frames))

    @ignore_warnings
    def test_recovery_should_redo_committed_transactions_full_flush(self):
        update_operations = [ObjectUpdateArguments('invert_color', 0, 99),
                            ObjectUpdateArguments('invert_color', 100, 199)
        ]

        dataframe_metadata = write_file(self.storage_engine, 'traffic001_6', include_lsn=True)

        video_frames = read_file_from_petastorm(self.storage_engine, dataframe_metadata)
        updated_video_frames = apply_update_to_dataframe(video_frames, update_operations[0])
        updated_video_frames = apply_update_to_dataframe(updated_video_frames, update_operations[1])

        buffer_mgr = BufferManager(200, self.storage_engine)
        log_mgr = LogicalLogManager(buffer_mgr)
        txn_mgr = OptimizedTransactionManager(storage_engine_passed=self.storage_engine,
                                    log_manager_passed=log_mgr,
                                    buffer_manager_passed=buffer_mgr)
        txn_id = txn_mgr.begin_transaction()
        txn_mgr.update_object(txn_id, dataframe_metadata, update_operations[0])
        txn_mgr.update_object(txn_id, dataframe_metadata, update_operations[1])
        txn_mgr.commit_transaction(txn_id)

        # Reset buffer manager
        buffer_mgr.flush_all_slots()
        buffer_mgr.discard_all_slots()
        # Recovery from log
        log_mgr.recover_log()

        # Check contents of buffer manager after recovery
        actual_updated_video_frames = pd.DataFrame()
        for i in range(4):
            batch = buffer_mgr.read_slot(dataframe_metadata, i)
            actual_updated_video_frames = actual_updated_video_frames.append(batch.frames, ignore_index=True)

        LoggingManager().log(f'Asserting buffer manager is updated', LoggingLevel.INFO)
        self.assertTrue(dataframes_equal(updated_video_frames, actual_updated_video_frames))
        self.assertFalse(dataframes_equal(video_frames, actual_updated_video_frames))

    @ignore_warnings
    def test_recovery_update_and_clr_in_log(self):
        update_operations = [ObjectUpdateArguments('invert_color', 0, 99),
                            ObjectUpdateArguments('invert_color', 100, 199)
        ]

        dataframe_metadata = write_file(self.storage_engine, 'traffic001_6', include_lsn=True)

        video_frames = read_file_from_petastorm(self.storage_engine, dataframe_metadata)
        updated_video_frames = apply_update_to_dataframe(video_frames, update_operations[0])
        updated_video_frames = apply_update_to_dataframe(updated_video_frames, update_operations[1])

        buffer_mgr = BufferManager(200, self.storage_engine)
        log_mgr = LogicalLogManager(buffer_mgr)
        txn_mgr = OptimizedTransactionManager(storage_engine_passed=self.storage_engine,
                                    log_manager_passed=log_mgr,
                                    buffer_manager_passed=buffer_mgr)
        txn_id = txn_mgr.begin_transaction()
        txn_mgr.update_object(txn_id, dataframe_metadata, update_operations[0])
        txn_mgr.update_object(txn_id, dataframe_metadata, update_operations[1])
        PressurePointManager().add_pressure_point(PressurePoint(
            PressurePointLocation.LOGICAL_LOG_MANAGER_ROLLBACK_AFTER_CLR,
            PressurePointBehavior.EARLY_RETURN
        ))
        txn_mgr.abort_transaction(txn_id)
        PressurePointManager().remove_pressure_point(PressurePoint(
            PressurePointLocation.LOGICAL_LOG_MANAGER_ROLLBACK_AFTER_CLR,
            PressurePointBehavior.EARLY_RETURN
        ))

        # Reset buffer manager
        buffer_mgr.discard_all_slots()
        # Recovery from log
        log_mgr.recover_log()

        # Check contents of buffer manager after recovery
        actual_updated_video_frames = pd.DataFrame()
        for i in range(4):
            batch = buffer_mgr.read_slot(dataframe_metadata, i)
            actual_updated_video_frames = actual_updated_video_frames.append(batch.frames, ignore_index=True)

        LoggingManager().log(f'Asserting buffer manager is updated', LoggingLevel.INFO)
        self.assertTrue(dataframes_equal(video_frames, actual_updated_video_frames))
        self.assertFalse(dataframes_equal(updated_video_frames, actual_updated_video_frames))

if __name__ == '__main__':
    unittest.main()     
