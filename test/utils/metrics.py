import os
import time
import timeit

from src.utils.logging_manager import LoggingLevel, LoggingManager

# To use more granular profiling:
#
# import cProfile
# with cProfile.Profile() as pr:
#   ...
# pr.print_stats()


class Timing:
    def __init__(self, desc):
        self.__desc = desc
        self.__start = 0.0
        self.__end = 0.0

    def __enter__(self):
        LoggingManager().log(f"Begin timing {self.__desc}", LoggingLevel.INFO)
        self.__start = time.time()
        return self

    def __exit__(self, type, value, traceback):
        self.__end = time.time()
        LoggingManager().log(f"Stop timing {self.__desc}", LoggingLevel.DEBUG)
        LoggingManager().log(f"{self.__desc} took {self.get_time():.6f} s", LoggingLevel.INFO)

    def get_time(self):
        return self.__end - self.__start


def benchmark(func, n_iterations=100):
    LoggingManager().log(f"Benchmark {n_iterations} iterations of {func.__name__}", LoggingLevel.DEBUG)
    execution_time = timeit.timeit(func, number=n_iterations)
    LoggingManager().log(f"Average runtime of {func.__name__}: {execution_time:.6f} s", LoggingLevel.INFO)

def get_dir_size(dir):
    total_size = 0
    for dirpath, _, filenames in os.walk(dir):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)
    return total_size
