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

class NumUpdatesBenchmarkPartitioned(AbstractBenchmark):
    def __init__(self, num_updates, hybrid_protocol, repetitions, storage_engine, dataframe_metadata, pphysical_logging=False):
        super().__init__(repetitions=repetitions)
        self.num_updates = num_updates
        self.hybrid_protocol = hybrid_protocol
        self.storage_engine = storage_engine
        self.dataframe_metadata = dataframe_metadata
        self.pphysical_logging = pphysical_logging

    def _setUp(self):
        clear_petastorm_storage_folder()
        clear_transaction_storage_folder()

        shutil.copytree(SHADOW_PETASTORM_STORAGE_FOLDER, PETASTORM_STORAGE_FOLDER, dirs_exist_ok=True)

        self.buffer_mgr = BufferManager(100, self.storage_engine)
        self.log_mgr = LogicalLogManager(self.buffer_mgr)
        self.txn_mgr = OptimizedTransactionManager(storage_engine_passed=self.storage_engine,
                                                    log_manager_passed=self.log_mgr,
                                                    buffer_manager_passed=self.buffer_mgr,
                                                    force_physical_logging=self.hybrid_protocol,
                                                    force_pphysical_logging=self.pphysical_logging)

        self.update_operations = [ObjectUpdateArguments('invert_color', 0, 4499) for i in range(self.num_updates)]

    def _tearDown(self):
        clear_petastorm_storage_folder()
        clear_transaction_storage_folder()

    def _run(self):
        txn_id = self.txn_mgr.begin_transaction()
        for update_operation in self.update_operations:
            self.txn_mgr.update_object(txn_id, self.dataframe_metadata, update_operation)
        self.txn_mgr.commit_transaction(txn_id)

        self.buffer_mgr.flush_all_slots()

class NumUpdatesBenchmark(AbstractBenchmark):
    def __init__(self, num_updates, storage_engine, dataframe_metadata):
        super().__init__()
        self.num_updates = num_updates
        self.storage_engine = storage_engine
        self.dataframe_metadata = dataframe_metadata

    def _setUp(self):
        clear_petastorm_storage_folder()
        clear_transaction_storage_folder()

        shutil.copytree(SHADOW_PETASTORM_STORAGE_FOLDER, PETASTORM_STORAGE_FOLDER, dirs_exist_ok=True)

        self.txn_mgr = TransactionManager(storage_engine_passed=self.storage_engine)

        self.update_operations = [ObjectUpdateArguments('invert_color', 0, 4499) for i in range(self.num_updates)]

    def _tearDown(self):
        clear_petastorm_storage_folder()
        clear_transaction_storage_folder()

    def _run(self):
        txn_id = self.txn_mgr.begin_transaction()
        for update_operation in self.update_operations:
            self.txn_mgr.update_object(txn_id, self.dataframe_metadata, update_operation)
        self.txn_mgr.commit_transaction(txn_id)

if __name__ == '__main__':
    LoggingManager().setEffectiveLevel(LoggingLevel.INFO)

    time_df = pd.DataFrame(columns=['protocol', 'num_updates', 'time'])
    disk_df = pd.DataFrame(columns=['protocol', 'num_updates', 'disk'])

    storage_engine, dataframe_metadata = setUp(True)
    # Logical logging
    for i in range(0, 9, 2):
        if i == 0:
            i = 1
        benchmark = NumUpdatesBenchmarkPartitioned(i, False, 5, storage_engine, dataframe_metadata)
        benchmark.run_benchmark()
        print(f'Timing: {benchmark.time_measurements}')
        print(f'Disk: {benchmark.disk_measurement}')
        for result in benchmark.time_measurements:
            time_df = time_df.append({'protocol': 'Logical', 'num_updates': i, 'time': result}, ignore_index=True)
        time_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/num_updates_time.csv')
        disk_df = disk_df.append({'protocol': 'Logical', 'num_updates': i, 'disk': benchmark.disk_measurement}, ignore_index=True)
        disk_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/num_updates_disk.csv')

    # Hybrid logging
    for i in range(0, 9, 2):
        if i == 0:
            i = 1
        benchmark = NumUpdatesBenchmarkPartitioned(i, True, 5, storage_engine, dataframe_metadata)
        benchmark.run_benchmark()
        print(f'Timing: {benchmark.time_measurements}')
        print(f'Disk: {benchmark.disk_measurement}')
        for result in benchmark.time_measurements:
            time_df = time_df.append({'protocol': 'Hybrid', 'num_updates': i, 'time': result}, ignore_index=True)
        time_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/num_updates_time.csv')
        disk_df = disk_df.append({'protocol': 'Hybrid', 'num_updates': i, 'disk': benchmark.disk_measurement}, ignore_index=True)
        disk_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/num_updates_disk.csv')
    
    # Physical logging (with buffering)
    for i in range(0, 9, 2):
        if i == 0:
            i = 1
        benchmark = NumUpdatesBenchmarkPartitioned(i, True, 5, storage_engine, dataframe_metadata, pphysical_logging=True)
        benchmark.run_benchmark()
        print(f'Timing: {benchmark.time_measurements}')
        print(f'Disk: {benchmark.disk_measurement}')
        for result in benchmark.time_measurements:
            time_df = time_df.append({'protocol': 'Physical', 'num_updates': i, 'time': result}, ignore_index=True)
        time_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/num_updates_time.csv')
        disk_df = disk_df.append({'protocol': 'Physical', 'num_updates': i, 'disk': benchmark.disk_measurement}, ignore_index=True)
        disk_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/num_updates_disk.csv')
    tearDown()

    # Physical logging
    storage_engine, dataframe_metadata = setUp(False)
    for i in range(0, 9, 2):
        if i == 0:
            i = 1
        benchmark = NumUpdatesBenchmark(i, storage_engine, dataframe_metadata)
        benchmark.run_benchmark()
        print(f'Timing: {benchmark.time_measurements}')
        print(f'Disk: {benchmark.disk_measurement}')
        for result in benchmark.time_measurements:
            time_df = time_df.append({'protocol': 'Physical (Unbuffered)', 'num_updates': i, 'time': result}, ignore_index=True)
        time_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/num_updates_time.csv')
        disk_df = disk_df.append({'protocol': 'Physical (Unbuffered)', 'num_updates': i, 'disk': benchmark.disk_measurement}, ignore_index=True)
        disk_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/num_updates_disk.csv')
    tearDown()
