from abc import ABCMeta, abstractmethod
from pathlib import Path
from typing import Iterator, Dict
import pandas as pd

from src.models.storage.batch import Batch


class AbstractReader(metaclass=ABCMeta):
    """
    Abstract class for defining data reader. All other video readers use this
    abstract class. Video readers are expected to return Batch
    in an iterative manner.
    Attributes:
        file_url (str): path to read data from
        batch_size (int, optional): No. of frames to read in batch from video
        offset (int, optional): Start frame location in video
        """

    def __init__(self, file_url: str, batch_size=None,
                 offset=None):
        # Opencv doesn't support pathlib.Path so convert to raw str
        if isinstance(file_url, Path):
            file_url = str(file_url)

        self.file_url = file_url
        self.batch_size = batch_size
        self.offset = offset

    def read(self) -> Iterator[Batch]:
        """
        This calls the sub class read implementation and
        yields the batch to the caller
        """

        data_batch = []
        # Fetch batch_size from Config if not provided
        if self.batch_size is None or self.batch_size < 0:
            if self.batch_size is None:
                self.batch_size = 50

        for data in self._read():
            data_batch.append(data)
            if len(data_batch) % self.batch_size == 0:
                yield Batch(pd.DataFrame(data_batch))
                data_batch = []
        if data_batch:
            yield Batch(pd.DataFrame(data_batch))

    @abstractmethod
    def _read(self) -> Iterator[Dict]:
        """
        Every sub class implements it's own logic
        to read the file and yields an object iterator.
        """