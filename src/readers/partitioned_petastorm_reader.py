from petastorm import make_reader
from typing import Iterator, Dict
import os

from src.readers.abstract_reader import AbstractReader
from src.utils.logging_manager import LoggingLevel, LoggingManager


class PartitionedPetastormReader(AbstractReader):
    def __init__(self, *args, cur_shard=None, shard_count=None,
                 predicate=None, group_num=None, **kwargs):
        """
        Reads data from the petastorm parquet stores. Note this won't
        work for any arbitary parquet store apart from one materialized
        using petastorm. In order to generalize, we might have to replace
        `make_reader` with `make_batch_reader`.
        Attributes:
            cur_shard (int, optional): Shard number to load from if sharded
            shard_count (int, optional): Specify total number of shards if
                                      applicable
            predicate (PredicateBase, optional): instance of predicate object
                to filter rows to be returned by reader
        """
        self.cur_shard = cur_shard
        self.shard_count = shard_count
        self.predicate = predicate
        self.group_num = group_num
        super().__init__(*args, **kwargs)
        if self.cur_shard is not None and self.cur_shard <= 0:
            self.cur_shard = None

        if self.shard_count is not None and self.shard_count <= 0:
            self.shard_count = None

    def _read(self) -> Iterator[Dict]:
        # `Todo`: Generalize this reader
        if(self.group_num == None):
            curr_group_num = 0
            while os.path.isdir(f'{self.file_url}/group{curr_group_num}'[6:]): # ignore file:/ at beginning
                yield from self._read_group(curr_group_num)
                curr_group_num = curr_group_num + 1
        else:
            yield from self._read_group(self.group_num)
    
    def _read_group(self, curr_group_num: int) -> Iterator[Dict]:
        with make_reader(f'{self.file_url}/group{curr_group_num}',
                        shard_count=self.shard_count,
                        cur_shard=self.cur_shard,
                        predicate=self.predicate) \
                as reader:
            for row in reader:
                # print(f'ROW: {row.id}')
                yield row._asdict()
