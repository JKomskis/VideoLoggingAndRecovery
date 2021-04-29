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

storage_engine = None
dataframe_metadata = None

class NumUpdatesBenchmarkPartitioned(AbstractBenchmark):
    def __init__(self, num_updates, hybrid_protocol, repetitions):
        super().__init__(repetitions=repetitions)
        self.num_updates = num_updates
        self.hybrid_protocol = hybrid_protocol
    
    def _setUp(self):
        clear_petastorm_storage_folder()
        clear_transaction_storage_folder()

        shutil.copytree(SHADOW_PETASTORM_STORAGE_FOLDER, PETASTORM_STORAGE_FOLDER, dirs_exist_ok=True)

        self.buffer_mgr = BufferManager(200, storage_engine)
        self.log_mgr = LogicalLogManager(self.buffer_mgr)
        self.txn_mgr = OptimizedTransactionManager(storage_engine_passed=storage_engine,
                                                    log_manager_passed=self.log_mgr,
                                                    buffer_manager_passed=self.buffer_mgr,
                                                    force_physical_logging=self.hybrid_protocol)
        
        self.update_operations = [ObjectUpdateArguments('invert_color', 100, 225) for i in range(self.num_updates)]

    def _tearDown(self):
        clear_petastorm_storage_folder()
        clear_transaction_storage_folder()

    def _run(self):
        txn_id = self.txn_mgr.begin_transaction()
        for update_operation in self.update_operations:
            self.txn_mgr.update_object(txn_id, dataframe_metadata, update_operation)
        self.txn_mgr.commit_transaction(txn_id)

        self.buffer_mgr.flush_all_slots()

class NumUpdatesBenchmark(AbstractBenchmark):
    def __init__(self, num_updates):
        super().__init__()
        self.num_updates = num_updates
    
    def _setUp(self):
        clear_petastorm_storage_folder()
        clear_transaction_storage_folder()

        shutil.copytree(SHADOW_PETASTORM_STORAGE_FOLDER, PETASTORM_STORAGE_FOLDER, dirs_exist_ok=True)

        self.txn_mgr = TransactionManager(storage_engine_passed=storage_engine)
        
        self.update_operations = [ObjectUpdateArguments('invert_color', 100, 225) for i in range(self.num_updates)]

    def _tearDown(self):
        clear_petastorm_storage_folder()
        clear_transaction_storage_folder()

    def _run(self):
        txn_id = self.txn_mgr.begin_transaction()
        for update_operation in self.update_operations:
            self.txn_mgr.update_object(txn_id, dataframe_metadata, update_operation)
        self.txn_mgr.commit_transaction(txn_id)

def setUpPartitioned():
    global storage_engine, dataframe_metadata
    storage_engine = PartitionedPetastormStorageEngine()
    dataframe_metadata = write_file(storage_engine, 'traffic001_150', include_lsn=True)
    shutil.copytree(PETASTORM_STORAGE_FOLDER, SHADOW_PETASTORM_STORAGE_FOLDER, dirs_exist_ok=True)

def setUpRegular():
    global storage_engine, dataframe_metadata
    storage_engine = PetastormStorageEngine()
    dataframe_metadata = write_file(storage_engine, 'traffic001_150')
    shutil.copytree(PETASTORM_STORAGE_FOLDER, SHADOW_PETASTORM_STORAGE_FOLDER, dirs_exist_ok=True)

def tearDown():
    shutil.rmtree(SHADOW_PETASTORM_STORAGE_FOLDER)

if __name__ == '__main__':
    LoggingManager().setEffectiveLevel(LoggingLevel.DEBUG)

    data_df = pd.DataFrame(columns=['protocol', 'num_updates', 'time'])

    setUpPartitioned()
    # Logical logging
    for i in range(0, 31, 5):
        if i == 0:
            i = 1
        benchmark = NumUpdatesBenchmarkPartitioned(i, False, 7)
        benchmark.run_benchmark()
        print(f'{benchmark.time_measurements}')
        for result in benchmark.time_measurements:
            data_df = data_df.append({'protocol': 'Logical', 'num_updates': i, 'time': result}, ignore_index=True)
        data_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/num_updates.csv')

    # Physical logging
    for i in range(0, 31, 5):
        if i == 0:
            i = 1
        benchmark = NumUpdatesBenchmarkPartitioned(i, True, 7)
        benchmark.run_benchmark()
        print(f'{benchmark.time_measurements}')
        for result in benchmark.time_measurements:
            data_df = data_df.append({'protocol': 'Hybrid', 'num_updates': i, 'time': result}, ignore_index=True)
        data_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/num_updates.csv')
    tearDown()

    setUpRegular()
    for i in range(0, 11, 2):
        if i == 0:
            i = 1
        benchmark = NumUpdatesBenchmark(i)
        benchmark.run_benchmark()
        print(f'{benchmark.time_measurements}')
        for result in benchmark.time_measurements:
            data_df = data_df.append({'protocol': 'Physical', 'num_updates': i, 'time': result}, ignore_index=True)
        data_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/num_updates.csv')
    tearDown()
