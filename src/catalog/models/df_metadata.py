from __future__ import annotations
import pickle

from src.catalog.df_schema import DataFrameSchema
from src.catalog.models.df_column import DataFrameColumn
from src.catalog.column_type import ColumnType
from pathlib import Path

class DataFrameMetadata():
    def __init__(self, name: str, file_url: str, identifier_id='id'):
        self._name = name
        self._file_url = file_url
        self._schema = None
        self._unique_identifier_column = identifier_id

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, column_list):
        self._schema = DataFrameSchema(self._name, column_list)

    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @property
    def file_url(self):
        return self._file_url

    @property
    def columns(self):
        return self._columns

    @property
    def identifier_column(self):
        return self._unique_identifier_column

    def __eq__(self, other):
        # return self.id == other.id and \
        return self.file_url == other.file_url and \
            self.schema == other.schema and \
            self.identifier_column == other.identifier_column and \
            self.name == other.name
    
    def serialize(self) -> bytes:
        height = 0
        width = 0
        has_lsn = False
        for column in self.schema.column_list:
            if column.name == 'data':
                height = column.array_dimensions[0]
                width = column.array_dimensions[1]
            elif column.name == 'lsn':
                has_lsn = True
        data = {
            'file_url': self._file_url,
            'height': height,
            'width': width,
            'has_lsn': has_lsn
        }
        return pickle.dumps(data)
    
    @classmethod
    def deserialize(cls, data) -> DataFrameMetadata:
        data_dict = pickle.loads(data)
        
        dataframe_metadata = DataFrameMetadata(Path(data_dict['file_url']).stem, data_dict['file_url'])
        dataframe_columns = [
            DataFrameColumn('id', ColumnType.INTEGER),
            DataFrameColumn('data', ColumnType.NDARRAY, array_dimensions= [data_dict['height'], data_dict['width'], 3]),
        ]
        if data_dict['has_lsn']:
            dataframe_columns.append(DataFrameColumn('lsn', ColumnType.INTEGER))
        dataframe_metadata.schema = dataframe_columns
        return dataframe_metadata