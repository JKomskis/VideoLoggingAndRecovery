from abc import ABC, abstractmethod

from src.config.constants import TRANSACTION_STORAGE_FOLDER
from src.utils.logging_manager import LoggingManager, LoggingLevel
from test.utils.metrics import Timing, get_dir_size

class AbstractBenchmark(ABC):
    def __init__(self, repetitions = 1):
        self.repetitions = repetitions
        self._time_measurements = []
        self._disk_measurement = 0

    def _setUp(self):
        pass

    @abstractmethod
    def _run(self):
        pass

    def _tearDown(self):
        pass

    def run_benchmark(self):
        for i in range(self.repetitions):
            LoggingManager().log(f'Running set up', LoggingLevel.INFO)
            self._setUp()
            with Timing(f'Run {i+1} of {self.repetitions}') as t:
                self._run()
            self.time_measurements.append(t.get_time())
            if i + 1 == self.repetitions:
                self._disk_measurement = get_dir_size(TRANSACTION_STORAGE_FOLDER)
            LoggingManager().log(f'Running tear down', LoggingLevel.INFO)
            self._tearDown()

    @property
    def time_measurements(self):
        return self._time_measurements

    @property
    def disk_measurement(self):
        return self._disk_measurement
