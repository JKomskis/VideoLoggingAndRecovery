import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '../..'))

from src.storage.partitioned_petastorm_storage_engine import PartitionedPetastormStorageEngine
from src.transaction.optimized_transaction_manager import OptimizedTransactionManager
from src.transaction.object_update_arguments import ObjectUpdateArguments
from test.utils.util_functions import write_file, \
                                        clear_petastorm_storage_folder, \
                                        clear_transaction_storage_folder
from src.Logging.logical_log_manager import LogicalLogManager
from src.buffer.buffer_manager import BufferManager
from src.utils.logging_manager import LoggingManager, LoggingLevel

from test.benchmark.abstract_benchmark import AbstractBenchmark

class NumUpdatesBenchmark(AbstractBenchmark):
    def __init__(self, num_updates):
        super().__init__()
        self.num_updates = num_updates
        self.storage_engine = PartitionedPetastormStorageEngine()
    
    def _setUp(self):
        clear_petastorm_storage_folder()
        clear_transaction_storage_folder()

        self.dataframe_metadata = write_file(self.storage_engine, 'traffic001_180', include_lsn=True)
        self.buffer_mgr = BufferManager(200, self.storage_engine)
        self.log_mgr = LogicalLogManager(buffer_mgr)
        self.txn_mgr = OptimizedTransactionManager(storage_engine_passed=self.storage_engine,
                                                    log_manager_passed=self.log_mgr,
                                                    buffer_manager_passed=self.buffer_mgr)
        
        self.update_operations = [ObjectUpdateArguments('invert_color', 125*i, 125*(i+1)-1) for i in range(self.num_updates)]


    def _tearDown(self):
        clear_petastorm_storage_folder()
        clear_transaction_storage_folder()

    def _run(self):
        txn_id = self.txn_mgr.begin_transaction()
        for update_operation in self.update_operations:
            self.txn_mgr.update_object(txn_id, dataframe_metadata, update_operation)
        self.txn_mgr.commit_transaction(txn_id)

        self.buffer_mgr.flush_all_slots()

if __name__ == '__main__':
    LoggingManager().setEffectiveLevel(LoggingLevel.DEBUG)
    results = {}
    for i in range(1, 57, 5):
        benchmark = NumUpdatesBenchmark(i)
        benchmark.run_benchmark()
        print(f'{benchmark.time_measurements}')
        results[i] = benchmark.time_measurements
    print(f'{results}')