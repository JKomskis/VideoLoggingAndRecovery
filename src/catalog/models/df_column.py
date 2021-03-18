from typing import List

from ast import literal_eval

from src.catalog.column_type import ColumnType

class DataFrameColumn():
    def __init__(self,
                 name: str,
                 type: ColumnType,
                 is_nullable: bool = False,
                 array_dimensions: List[int] = [],
                 metadata_id: int = None):
        self._name = name
        self._type = type
        self._is_nullable = is_nullable
        self._array_dimensions = str(array_dimensions)
        self._metadata_id = metadata_id

    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @property
    def type(self):
        return self._type

    @property
    def is_nullable(self):
        return self._is_nullable

    @property
    def array_dimensions(self):
        return literal_eval(self._array_dimensions)

    @array_dimensions.setter
    def array_dimensions(self, value):
        self._array_dimensions = str(value)

    @property
    def metadata_id(self):
        return self._metadata_id

    @metadata_id.setter
    def metadata_id(self, value):
        self._metadata_id = value

    def __str__(self):
        column_str = "Column: (%s, %s, %s, " % (self._name,
                                                self._type.name,
                                                self._is_nullable)

        column_str += "["
        column_str += ', '.join(['%d'] * len(self.array_dimensions)) \
                      % tuple(self.array_dimensions)
        column_str += "])"

        return column_str

    def __eq__(self, other):
        return self.id == other.id and \
            self.metadata_id == other.metadata_id and \
            self.is_nullable == other.is_nullable and \
            self.array_dimensions == other.array_dimensions and \
            self.name == other.name and \
            self.type == other.type