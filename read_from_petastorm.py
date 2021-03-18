from src.readers.opencv_reader import OpenCVReader
from src.storage.petastorm_storage_engine import PetastormStorageEngine
from src.catalog.models.df_metadata import DataFrameMetadata
from src.catalog.models.df_column import DataFrameColumn
from src.catalog.column_type import ColumnType
from src.models.storage.batch import Batch
import pandas as pd
import cv2

from src.config.constants import \
    INPUT_VIDEO_FOLDER

video_name = 'traffic001_30'
dataframe_metadata = DataFrameMetadata(video_name, f'{INPUT_VIDEO_FOLDER}/{video_name}.mp4')
dataframe_columns = [
    DataFrameColumn('id', ColumnType.INTEGER),
    DataFrameColumn('data', ColumnType.NDARRAY, array_dimensions= [540, 960, 3])
]
dataframe_metadata.schema = dataframe_columns

storage_engine = PetastormStorageEngine()

frames = pd.DataFrame(columns=list(['data', 'id']))
for batch in storage_engine.read(dataframe_metadata):
    print(f'BATCH: {batch.frames.data[0].shape}')
    frames = frames.append(batch.frames)

print(frames.columns)
frames.sort_values(by='id')

print(type(frames.data[0]))
width = frames.data[0].shape[1]
height = frames.data[0].shape[0]
fps = 30
fourcc = cv2.VideoWriter_fourcc(*'mp4v')
output = cv2.VideoWriter(f'{video_name}_{length}.mp4', fourcc, fps, (width, height))

for frame in frames.data:
    output.write(frame)
output.release
