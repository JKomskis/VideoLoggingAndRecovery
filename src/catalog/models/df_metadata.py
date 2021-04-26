from src.catalog.df_schema import DataFrameSchema

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