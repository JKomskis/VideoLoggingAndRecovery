from abc import ABC, abstractmethod
from cProfile import Profile
from subprocess import run, PIPE
from sys import executable

from src.config.constants import BENCHMARK_DATA_FOLDER
from src.utils.logging_manager import LoggingManager, LoggingLevel

class AbstractProfile(ABC):
    def __init__(self, profile_name):
        self.profile_name = profile_name

    def _setUp(self):
        pass

    @abstractmethod
    def _run(self):
        pass

    def _tearDown(self):
        pass

    def run_profile(self):
        LoggingManager().log(f'Running set up for {self.profile_name}', LoggingLevel.INFO)
        self._setUp()
        with Profile() as prof:
            self._run()

        profile_dump = f'{BENCHMARK_DATA_FOLDER}/{self.profile_name}.prof'
        prof.dump_stats(profile_dump)

        LoggingManager().log(f'Running tear down for {self.profile_name}', LoggingLevel.INFO)
        self._tearDown()

        LoggingManager().log(f'Creating flamegraph of {self.profile_name}', LoggingLevel.INFO)
        # flameprof only has a documented command line interface
        svg = run(['flameprof', profile_dump], stdout=PIPE)
        with open(f'{BENCHMARK_DATA_FOLDER}/{self.profile_name}.svg', 'w') as svg_file:
            svg_file.write(svg.stdout.decode('utf8'))
