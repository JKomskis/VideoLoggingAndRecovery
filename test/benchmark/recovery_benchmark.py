import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '../..'))
import shutil
import pandas as pd

from src.storage.partitioned_petastorm_storage_engine import PartitionedPetastormStorageEngine
from src.storage.petastorm_storage_engine import PetastormStorageEngine
from src.transaction.optimized_transaction_manager import OptimizedTransactionManager
from src.transaction.transaction_manager import TransactionManager
from src.transaction.object_update_arguments import ObjectUpdateArguments
from test.utils.util_functions import write_file, \
                                        clear_petastorm_storage_folder, \
                                        clear_transaction_storage_folder
from src.Logging.logical_log_manager import LogicalLogManager
from src.Logging.log_manager import LogManager
from src.buffer.buffer_manager import BufferManager
from src.utils.logging_manager import LoggingManager, LoggingLevel
from src.config.constants import SHADOW_PETASTORM_STORAGE_FOLDER, \
                                PETASTORM_STORAGE_FOLDER, \
                                BENCHMARK_DATA_FOLDER

from test.benchmark.abstract_benchmark import AbstractBenchmark

from test.benchmark.benchmark_environment import setUp, tearDown

class CommitBenchmarkPartitioned(AbstractBenchmark):
    def __init__(self, num_commits, hybrid_protocol, repetitions, storage_engine, dataframe_metadata):
        super().__init__(repetitions=repetitions)
        self.num_commits = num_commits
        self.hybrid_protocol = hybrid_protocol
        self.storage_engine = storage_engine
        self.dataframe_metadata = dataframe_metadata

    def _setUp(self):
        clear_petastorm_storage_folder()
        clear_transaction_storage_folder()

        shutil.copytree(SHADOW_PETASTORM_STORAGE_FOLDER, PETASTORM_STORAGE_FOLDER, dirs_exist_ok=True)

        self.buffer_mgr = BufferManager(200, self.storage_engine)
        self.log_mgr = LogicalLogManager(self.buffer_mgr)
        self.txn_mgr = OptimizedTransactionManager(storage_engine_passed=self.storage_engine,
                                                    log_manager_passed=self.log_mgr,
                                                    buffer_manager_passed=self.buffer_mgr,
                                                    force_physical_logging=self.hybrid_protocol)

        txn_id = self.txn_mgr.begin_transaction()
        for update_operation in [
            ObjectUpdateArguments('invert_color', 0, 4499) for i in range(self.num_commits)
        ]:
            self.txn_mgr.update_object(txn_id, self.dataframe_metadata, update_operation)
        self.txn_mgr.commit_transaction(txn_id)

        # Simulate restart after a crash
        self.buffer_mgr = BufferManager(200, self.storage_engine)
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

    data_df = pd.DataFrame(columns=['protocol', 'num_commits', 'time'])

    storage_engine, dataframe_metadata = setUp(True)

    # Logical logging
    for i in range(0, 9, 2):
        if i == 0:
            i = 1
        benchmark = CommitBenchmarkPartitioned(i, False, 5, storage_engine, dataframe_metadata)
        benchmark.run_benchmark()
        print(f'{benchmark.time_measurements}')
        for result in benchmark.time_measurements:
            data_df = data_df.append({'protocol': 'Logical', 'num_commits': i, 'time': result}, ignore_index=True)
        data_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/num_commits.csv')

    # Hybrid logging
    for i in range(0, 9, 2):
        if i == 0:
            i = 1
        benchmark = CommitBenchmarkPartitioned(i, True, 5, storage_engine, dataframe_metadata)
        benchmark.run_benchmark()
        print(f'{benchmark.time_measurements}')
        for result in benchmark.time_measurements:
            data_df = data_df.append({'protocol': 'Hybrid', 'num_commits': i, 'time': result}, ignore_index=True)
        data_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/num_commits.csv')
    tearDown()