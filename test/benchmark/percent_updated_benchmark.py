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

class PercentUpdatedBenchmarkPartitioned(AbstractBenchmark):
    def __init__(self, percent_updated, hybrid_protocol, repetitions, storage_engine, dataframe_metadata):
        super().__init__(repetitions=repetitions)
        self.percent_updated = percent_updated
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
        
        self.update_operation = ObjectUpdateArguments('invert_color', 0, int(4499*(self.percent_updated/100)))

    def _tearDown(self):
        clear_petastorm_storage_folder()
        clear_transaction_storage_folder()

    def _run(self):
        txn_id = self.txn_mgr.begin_transaction()
        self.txn_mgr.update_object(txn_id, self.dataframe_metadata, self.update_operation)
        self.txn_mgr.commit_transaction(txn_id)

        self.buffer_mgr.flush_all_slots()

class PercentUpdatedBenchmark(AbstractBenchmark):
    def __init__(self, percent_updated, repetitions, storage_engine, dataframe_metadata):
        super().__init__(repetitions=repetitions)
        self.percent_updated = percent_updated
        self.storage_engine = storage_engine
        self.dataframe_metadata = dataframe_metadata
    
    def _setUp(self):
        clear_petastorm_storage_folder()
        clear_transaction_storage_folder()

        shutil.copytree(SHADOW_PETASTORM_STORAGE_FOLDER, PETASTORM_STORAGE_FOLDER, dirs_exist_ok=True)

        self.txn_mgr = TransactionManager(storage_engine_passed=self.storage_engine)
        
        self.update_operation = ObjectUpdateArguments('invert_color', 0, int(4499*(self.percent_updated/100)))

    def _tearDown(self):
        clear_petastorm_storage_folder()
        clear_transaction_storage_folder()

    def _run(self):
        txn_id = self.txn_mgr.begin_transaction()
        self.txn_mgr.update_object(txn_id, self.dataframe_metadata, self.update_operation)
        self.txn_mgr.commit_transaction(txn_id)

if __name__ == '__main__':
    LoggingManager().setEffectiveLevel(LoggingLevel.INFO)

    data_df = pd.DataFrame(columns=['protocol', 'percent_updated', 'time'])

    storage_engine, dataframe_metadata = setUp(True)
    # Logical logging
    for i in range(10, 101, 10):
        benchmark = PercentUpdatedBenchmarkPartitioned(i, False, 5, storage_engine, dataframe_metadata)
        benchmark.run_benchmark()
        print(f'{benchmark.time_measurements}')
        for result in benchmark.time_measurements:
            data_df = data_df.append({'protocol': 'Logical', 'percent_updated': i, 'time': result}, ignore_index=True)
        data_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/percent_updated.csv')

    # Hybrid logging
    for i in range(10, 101, 10):
        benchmark = PercentUpdatedBenchmarkPartitioned(i, True, 5, storage_engine, dataframe_metadata)
        benchmark.run_benchmark()
        print(f'{benchmark.time_measurements}')
        for result in benchmark.time_measurements:
            data_df = data_df.append({'protocol': 'Hybrid', 'percent_updated': i, 'time': result}, ignore_index=True)
        data_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/percent_updated.csv')
    tearDown()

    # Physical logging
    storage_engine, dataframe_metadata = setUp(False)
    for i in range(10, 101, 10):
        benchmark = PercentUpdatedBenchmark(i, 3, storage_engine, dataframe_metadata)
        benchmark.run_benchmark()
        print(f'{benchmark.time_measurements}')
        for result in benchmark.time_measurements:
            data_df = data_df.append({'protocol': 'Physical', 'percent_updated': i, 'time': result}, ignore_index=True)
        data_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/percent_updated.csv')
    tearDown()
