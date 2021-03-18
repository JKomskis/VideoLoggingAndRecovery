from src.readers.opencv_reader import OpenCVReader
from src.storage.petastorm_storage_engine import PetastormStorageEngine
from src.catalog.models.df_metadata import DataFrameMetadata
from src.catalog.models.df_column import DataFrameColumn
from src.catalog.column_type import ColumnType
from src.models.storage.batch import Batch

from src.config.constants import \
    INPUT_VIDEO_FOLDER

dataframe_metadata = DataFrameMetadata('traffic001_30', f'{INPUT_VIDEO_FOLDER}/traffic001_30.mp4')

reader = OpenCVReader(file_url = dataframe_metadata.file_url, batch_size = 20)

dataframe_columns = [
    DataFrameColumn('id', ColumnType.INTEGER),
    DataFrameColumn('data', ColumnType.NDARRAY, array_dimensions= [reader.video_height(), reader.video_width(), 3])
]
dataframe_metadata.schema = dataframe_columns

storage_engine = PetastormStorageEngine()
storage_engine.create(dataframe_metadata)

for batch in reader.read():
    storage_engine.write(dataframe_metadata, batch)
