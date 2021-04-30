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

class RecoveryBenchmarkPartitioned(AbstractBenchmark):
    def __init__(self, should_commit, num_updates, hybrid_protocol, repetitions, storage_engine, dataframe_metadata):
        super().__init__(repetitions=repetitions)
        self.should_commit = should_commit
        self.num_updates = num_updates
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
            ObjectUpdateArguments('invert_color', 0, 4499) for i in range(self.num_updates)
        ]:
            self.txn_mgr.update_object(txn_id, self.dataframe_metadata, update_operation)
        if self.should_commit:
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

class RecoveryBenchmark(AbstractBenchmark):
    def __init__(self, should_commit, num_updates, repetitions, storage_engine, dataframe_metadata):
        super().__init__(repetitions=repetitions)
        self.should_commit = should_commit
        self.num_updates = num_updates
        self.storage_engine = storage_engine
        self.dataframe_metadata = dataframe_metadata

    def _setUp(self):
        clear_petastorm_storage_folder()
        clear_transaction_storage_folder()

        shutil.copytree(SHADOW_PETASTORM_STORAGE_FOLDER, PETASTORM_STORAGE_FOLDER, dirs_exist_ok=True)

        self.txn_mgr = TransactionManager(storage_engine_passed=self.storage_engine)

        txn_id = self.txn_mgr.begin_transaction()
        for update_operation in [
            ObjectUpdateArguments('invert_color', 0, 4499) for i in range(self.num_updates)
        ]:
            self.txn_mgr.update_object(txn_id, self.dataframe_metadata, update_operation)
        if self.should_commit:
            self.txn_mgr.commit_transaction(txn_id)

        # Simulate restart after a crash
        self.txn_mgr = TransactionManager(storage_engine_passed=self.storage_engine)

    def _tearDown(self):
        clear_petastorm_storage_folder()
        clear_transaction_storage_folder()

    def _run(self):
        self.txn_mgr.recover()


ITERATIONS = 5

if __name__ == '__main__':
    LoggingManager().setEffectiveLevel(LoggingLevel.DEBUG)

    commit_df = pd.DataFrame(columns=['protocol', 'num_commits', 'time'])
    abort_df = pd.DataFrame(columns=['protocol', 'num_aborts', 'time'])

    storage_engine, dataframe_metadata = setUp(True)

    # Logical logging
    for i in range(0, 9, 2):
        if i == 0:
            i = 1
        benchmark = RecoveryBenchmarkPartitioned(True, i, False, ITERATIONS, storage_engine, dataframe_metadata)
        benchmark.run_benchmark()
        print(f'Timing: {benchmark.time_measurements}')
        print(f'Disk: {benchmark.disk_measurement}')
        for result in benchmark.time_measurements:
            commit_df = commit_df.append({'protocol': 'Logical', 'num_commits': i, 'time': result}, ignore_index=True)
        commit_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/num_commits.csv')
    for i in range(0, 9, 2):
        if i == 0:
            i = 1
        benchmark = RecoveryBenchmarkPartitioned(False, i, False, ITERATIONS, storage_engine, dataframe_metadata)
        benchmark.run_benchmark()
        print(f'Timing: {benchmark.time_measurements}')
        print(f'Disk: {benchmark.disk_measurement}')
        for result in benchmark.time_measurements:
            abort_df = abort_df.append({'protocol': 'Logical', 'num_aborts': i, 'time': result}, ignore_index=True)
        abort_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/num_aborts.csv')

    # Hybrid logging
    for i in range(0, 9, 2):
        if i == 0:
            i = 1
        benchmark = RecoveryBenchmarkPartitioned(True, i, True, ITERATIONS, storage_engine, dataframe_metadata)
        benchmark.run_benchmark()
        print(f'Timing: {benchmark.time_measurements}')
        print(f'Disk: {benchmark.disk_measurement}')
        for result in benchmark.time_measurements:
            commit_df = commit_df.append({'protocol': 'Hybrid', 'num_commits': i, 'time': result}, ignore_index=True)
        commit_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/num_commits.csv')
    for i in range(0, 9, 2):
        if i == 0:
            i = 1
        benchmark = RecoveryBenchmarkPartitioned(False, i, True, ITERATIONS, storage_engine, dataframe_metadata)
        benchmark.run_benchmark()
        print(f'Timing: {benchmark.time_measurements}')
        print(f'Disk: {benchmark.disk_measurement}')
        for result in benchmark.time_measurements:
            abort_df = abort_df.append({'protocol': 'Hybrid', 'num_aborts': i, 'time': result}, ignore_index=True)
        abort_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/num_aborts.csv')
    tearDown()

    storage_engine, dataframe_metadata = setUp(False)

    # Physical logging
    for i in range(0, 9, 2):
        if i == 0:
            i = 1
        benchmark = RecoveryBenchmark(True, i, 1, storage_engine, dataframe_metadata)
        benchmark.run_benchmark()
        print(f'Timing: {benchmark.time_measurements}')
        print(f'Disk: {benchmark.disk_measurement}')
        for result in benchmark.time_measurements:
            commit_df = commit_df.append({'protocol': 'Physical', 'num_commits': i, 'time': result}, ignore_index=True)
        commit_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/num_commits.csv')
    for i in range(0, 9, 2):
        if i == 0:
            i = 1
        benchmark = RecoveryBenchmark(False, i, 1, storage_engine, dataframe_metadata)
        benchmark.run_benchmark()
        print(f'Timing: {benchmark.time_measurements}')
        print(f'Disk: {benchmark.disk_measurement}')
        for result in benchmark.time_measurements:
            abort_df = abort_df.append({'protocol': 'Physical', 'num_aborts': i, 'time': result}, ignore_index=True)
        abort_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/num_aborts.csv')

    tearDown()
