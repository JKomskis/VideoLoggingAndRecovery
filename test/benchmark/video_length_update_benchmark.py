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

class VideoLengthUpdateBenchmarkPartitioned(AbstractBenchmark):
    def __init__(self, len_sec, hybrid_protocol, repetitions):
        super().__init__(repetitions=repetitions)
        self.hybrid_protocol = hybrid_protocol
    
    def _setUp(self):
        clear_petastorm_storage_folder()
        clear_transaction_storage_folder()

        shutil.copytree(SHADOW_PETASTORM_STORAGE_FOLDER, PETASTORM_STORAGE_FOLDER, dirs_exist_ok=True)

        self.buffer_mgr = BufferManager(100, storage_engine)
        self.log_mgr = LogicalLogManager(self.buffer_mgr)
        self.txn_mgr = OptimizedTransactionManager(storage_engine_passed=storage_engine,
                                                    log_manager_passed=self.log_mgr,
                                                    buffer_manager_passed=self.buffer_mgr,
                                                    force_physical_logging=self.hybrid_protocol)
        
        self.update_operation = ObjectUpdateArguments('invert_color', 0, len_sec*30)

    def _tearDown(self):
        clear_petastorm_storage_folder()
        clear_transaction_storage_folder()

    def _run(self):
        txn_id = self.txn_mgr.begin_transaction()
        self.txn_mgr.update_object(txn_id, dataframe_metadata, self.update_operation)
        self.txn_mgr.commit_transaction(txn_id)

        self.buffer_mgr.flush_all_slots()

class VideoLengthUpdateBenchmark(AbstractBenchmark):
    def __init__(self, len_sec, repetitions):
        super().__init__(repetitions=repetitions)
    
    def _setUp(self):
        clear_petastorm_storage_folder()
        clear_transaction_storage_folder()

        shutil.copytree(SHADOW_PETASTORM_STORAGE_FOLDER, PETASTORM_STORAGE_FOLDER, dirs_exist_ok=True)

        self.txn_mgr = TransactionManager(storage_engine_passed=storage_engine)
        
        self.update_operation = ObjectUpdateArguments('invert_color', 0, len_sec*30)

    def _tearDown(self):
        clear_petastorm_storage_folder()
        clear_transaction_storage_folder()

    def _run(self):
        txn_id = self.txn_mgr.begin_transaction()
        self.txn_mgr.update_object(txn_id, dataframe_metadata, self.update_operation)
        self.txn_mgr.commit_transaction(txn_id)

def setUpPartitioned(len_sec):
    global storage_engine, dataframe_metadata
    storage_engine = PartitionedPetastormStorageEngine()
    dataframe_metadata = write_file(storage_engine, f'traffic001_{len_sec}', include_lsn=True)
    shutil.copytree(PETASTORM_STORAGE_FOLDER, SHADOW_PETASTORM_STORAGE_FOLDER, dirs_exist_ok=True)

def setUpRegular(len_sec):
    global storage_engine, dataframe_metadata
    storage_engine = PetastormStorageEngine()
    dataframe_metadata = write_file(storage_engine, f'traffic001_{len_sec}')
    shutil.copytree(PETASTORM_STORAGE_FOLDER, SHADOW_PETASTORM_STORAGE_FOLDER, dirs_exist_ok=True)

def tearDown():
    shutil.rmtree(SHADOW_PETASTORM_STORAGE_FOLDER)

if __name__ == '__main__':
    LoggingManager().setEffectiveLevel(LoggingLevel.INFO)

    data_df = pd.DataFrame(columns=['protocol', 'video_length', 'time'])

    for len_sec in [6, 30, 60, 120, 150, 180, 240, 300]:
        setUpPartitioned(len_sec)

        # Logical logging
        benchmark = VideoLengthUpdateBenchmarkPartitioned(len_sec, False, 5)
        benchmark.run_benchmark()
        print(f'{benchmark.time_measurements}')
        for result in benchmark.time_measurements:
            data_df = data_df.append({'protocol': 'Logical', 'video_length': len_sec, 'time': result}, ignore_index=True)
        data_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/video_length_update.csv')

        # Hybrid logging
        benchmark = VideoLengthUpdateBenchmarkPartitioned(len_sec, True, 5)
        benchmark.run_benchmark()
        print(f'{benchmark.time_measurements}')
        for result in benchmark.time_measurements:
            data_df = data_df.append({'protocol': 'Hybrid', 'video_length': len_sec, 'time': result}, ignore_index=True)
        data_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/video_length_update.csv')
        
        tearDown()

    # Physical logging
    for len_sec in [6, 30, 60, 120, 150, 180, 240, 300]:
        setUpRegular(len_sec)

        benchmark = VideoLengthUpdateBenchmark(len_sec, 3)
        benchmark.run_benchmark()
        print(f'{benchmark.time_measurements}')
        for result in benchmark.time_measurements:
            data_df = data_df.append({'protocol': 'Physical', 'video_length': len_sec, 'time': result}, ignore_index=True)
        data_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/video_length_update.csv')
        
        tearDown()
