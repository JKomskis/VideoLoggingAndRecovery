import numpy as np
import os
from typing import Iterator, List
from petastorm.codecs import CompressedImageCodec, NdarrayCodec, ScalarCodec
from petastorm.etl.dataset_metadata import materialize_dataset
from petastorm.unischema import Unischema, UnischemaField, dict_to_spark_row
from petastorm.predicates import in_lambda
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.types import IntegerType
import concurrent.futures

from src.catalog.models.df_metadata import DataFrameMetadata
from src.models.storage.batch import Batch
from src.readers.partitioned_petastorm_reader import PartitionedPetastormReader
from src.pressure_point.pressure_point_manager import PressurePointManager
from src.pressure_point.pressure_point import PressurePoint, PressurePointLocation, PressurePointBehavior
from src.utils.logging_manager import LoggingLevel, LoggingManager
from src.config.constants import \
    PETASTORM_STORAGE_FOLDER, \
    INPUT_VIDEO_FOLDER

class BufferManagerSlot():
    def __init__(self, dataframe_metadata, rows: Batch):
        self._dataframe_metadata = dataframe_metadata
        self._rows = rows
        self._dirty = False
    
    @property
    def dataframe_metadata(self):
        return self._dataframe_metadata
    
    @dataframe_metadata.setter
    def dataframe_metadata(self, dataframe_metadata):
        self._dataframe_metadata = dataframe_metadata
    
    @property
    def rows(self):
        return self._rows
    
    @rows.setter
    def rows(self, rows):
        self._rows = rows
        self.dirty = True

    @property
    def dirty(self):
        return self._dirty
    
    @dirty.setter
    def dirty(self, dirty):
        self._dirty = dirty


class BufferManager():
    def __init__(self, size, storage_engine):
        self._size = size
        self._slots = [None] * size
        self._storage_engine = storage_engine
        # beginning of list denotes least recently used, end of list denotes most recently used
        self._lru = []
    
    def _get_slot(self, table: DataFrameMetadata, group_num: int) -> (BufferManagerSlot, int):
        i = 0
        while i < len(self._slots):
            if self._slots[i] != None \
                and table == self._slots[i].dataframe_metadata \
                and group_num == self._slots[i].rows.get_group_num():
                    return self._slots[i], i
            i = i + 1
        return None, None
    
    def _get_free_slot(self) -> int:
        i = 0
        while i < len(self._slots):
            if self._slots[i] == None:
                return i
            i = i + 1
        
        to_evict = self._lru[0]
        self.flush_slot(to_evict)
        self.discard_slot(to_evict)
        return to_evict
    
    def _update_lru(self, slot_num: int) -> None:
        self._lru = list(filter(lambda curr_slot_num: curr_slot_num != slot_num, self._lru)) + [slot_num]
    
    def write_slot(self, table: DataFrameMetadata, rows: Batch) -> None:
        LoggingManager().log(f'Writing table {table.file_url} group {rows.get_group_num()}', LoggingLevel.INFO)
        slot, slot_num = self._get_slot(table, rows.get_group_num())
        if slot == None:
            LoggingManager().log(f'Getting table from storage engine', LoggingLevel.INFO)
            batch = list(self._storage_engine.read(table, group_num=rows.get_group_num()))[0]
            slot_num = self._get_free_slot()
            self._slots[slot_num] = BufferManagerSlot(table, batch)

        df = self._slots[slot_num].rows.frames
        for index, row in rows.frames.iterrows():
            for column in rows.frames.columns:
                if column == 'id':
                    continue
                df.loc[df.id == row.id, column] = [row[column]]
        
        self._slots[slot_num].dirty = True
        self._update_lru(slot_num)

    def read_slot(self, table: DataFrameMetadata, group_num) -> Batch:
        slot, slot_num = self._get_slot(table, group_num)
        batch = None
        if slot == None:
            LoggingManager().log(f'Reading table {table.file_url} group {group_num} from storage engine', LoggingLevel.INFO)
            batch = list(self._storage_engine.read(table, group_num=group_num))[0]
            slot_num = self._get_free_slot()
            self._slots[slot_num] = BufferManagerSlot(table, batch)
            LoggingManager().log(f'Reading into slot {slot_num}', LoggingLevel.INFO)
        else:
            batch = self._slots[slot_num].rows

        self._update_lru(slot_num)
        return batch
    
    def flush_slot(self, slot_num: int) -> None:
        if self._slots[slot_num] != None and self._slots[slot_num].dirty:
            LoggingManager().log(f'Flushing slot {slot_num}', LoggingLevel.INFO)
            self._storage_engine.write(self._slots[slot_num].dataframe_metadata, self._slots[slot_num].rows)
            self._slots[slot_num].dirty = False

    def flush_all_slots(self) -> None:
        LoggingManager().log(f'Flushing buffer manager', LoggingLevel.INFO)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(self.flush_slot, i): i for i in range(len(self._slots))}
            concurrent.futures.as_completed(futures)

    def discard_slot(self, slot_num: int) -> None:
        self._slots[slot_num] = None
    
    def discard_all_slots(self) -> None:
        LoggingManager().log(f'Resetting buffer manager', LoggingLevel.INFO)
        i = 0
        while i < len(self._slots):
            self.discard_slot(i)
            i = i + 1
    
    def get_group_lsn(self, table: DataFrameMetadata, group_num: int):
        batch = self.read_slot(table, group_num)
        batch.frames['lsn'].max()
