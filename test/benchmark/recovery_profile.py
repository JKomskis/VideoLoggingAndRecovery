import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '../..'))
import shutil

from src.transaction.optimized_transaction_manager import OptimizedTransactionManager
from src.transaction.transaction_manager import TransactionManager
from src.transaction.object_update_arguments import ObjectUpdateArguments
from test.utils.util_functions import write_file, \
                                        clear_petastorm_storage_folder, \
                                        clear_transaction_storage_folder
from src.Logging.logical_log_manager import LogicalLogManager
from src.buffer.buffer_manager import BufferManager
from src.utils.logging_manager import LoggingManager, LoggingLevel
from src.config.constants import SHADOW_PETASTORM_STORAGE_FOLDER, \
                                PETASTORM_STORAGE_FOLDER

from test.benchmark.abstract_profile import AbstractProfile

from test.benchmark.benchmark_environment import setUp, tearDown

NUM_UPDATES = 2

class RecoveryProfilePartitioned(AbstractProfile):
    def __init__(self, hybrid_protocol, storage_engine, dataframe_metadata):
        super().__init__(profile_name=('recovery_hybrid' if hybrid_protocol else 'recovery_logical'))
        self.hybrid_protocol = hybrid_protocol
        self.storage_engine = storage_engine
        self.dataframe_metadata = dataframe_metadata

    def _setUp(self):
        clear_petastorm_storage_folder()
        clear_transaction_storage_folder()

        shutil.copytree(SHADOW_PETASTORM_STORAGE_FOLDER, PETASTORM_STORAGE_FOLDER, dirs_exist_ok=True)

        self.buffer_mgr = BufferManager(100, self.storage_engine)
        self.log_mgr = LogicalLogManager(self.buffer_mgr)
        self.txn_mgr = OptimizedTransactionManager(storage_engine_passed=self.storage_engine,
                                                    log_manager_passed=self.log_mgr,
                                                    buffer_manager_passed=self.buffer_mgr,
                                                    force_physical_logging=self.hybrid_protocol)

        txn_id = self.txn_mgr.begin_transaction()
        for update_operation in [
            ObjectUpdateArguments('invert_color', 0, 4499) for i in range(NUM_UPDATES)
        ]:
            self.txn_mgr.update_object(txn_id, self.dataframe_metadata, update_operation)
        self.txn_mgr.commit_transaction(txn_id)

        # Simulate restart after a crash
        self.buffer_mgr = BufferManager(100, self.storage_engine)
        self.log_mgr = LogicalLogManager(self.buffer_mgr)
        self.txn_mgr = OptimizedTransactionManager(storage_engine_passed=self.storage_engine,
                                                    log_manager_passed=self.log_mgr,
                                                    buffer_manager_passed=self.buffer_mgr,
                                                    force_physical_logging=self.hybrid_protocol)

    def _tearDown(self):
        clear_petastorm_storage_folder()
        clear_transaction_storage_folder()

    def _run(self):
        self.txn_mgr.recover()

if __name__ == '__main__':
    LoggingManager().setEffectiveLevel(LoggingLevel.DEBUG)

    storage_engine, dataframe_metadata = setUp(True)

    # Logical logging
    profile = RecoveryProfilePartitioned(False, storage_engine, dataframe_metadata)
    profile.run_profile()

    # Hybrid logging
    profile = RecoveryProfilePartitioned(True, storage_engine, dataframe_metadata)
    profile.run_profile()
