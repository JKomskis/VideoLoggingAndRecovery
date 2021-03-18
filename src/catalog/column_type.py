from enum import Enum


class ColumnType(Enum):
    BOOLEAN = 1
    INTEGER = 2
    FLOAT = 3
    TEXT = 4
    NDARRAY = 5