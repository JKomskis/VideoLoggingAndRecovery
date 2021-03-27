import argparse

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

def init_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        usage='%(prog)s FILE OUTPUT_FILE',
        description="Save video from petastorm to a file for testing purposes."
    )
    parser.add_argument('file')
    parser.add_argument('output_file')
    return parser

def read_file(file_name: str, output_file: str) -> None:
    dataframe_metadata = DataFrameMetadata(file_name, f'{INPUT_VIDEO_FOLDER}/{file_name}.mp4')
    dataframe_columns = [
        DataFrameColumn('id', ColumnType.INTEGER),
        DataFrameColumn('data', ColumnType.NDARRAY, array_dimensions= [540, 960, 3])
    ]
    dataframe_metadata.schema = dataframe_columns

    storage_engine = PetastormStorageEngine()

    frames = None
    for batch in storage_engine.read(dataframe_metadata):
        if frames is None:
            frames = batch.frames
        else:
            frames = frames.append(batch.frames)

    frames = frames.sort_values(by='id')

    width = frames.data.iloc[0].shape[1]
    height = frames.data.iloc[0].shape[0]
    fps = 30
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    output = cv2.VideoWriter(output_file, fourcc, fps, (width, height))

    for frame in frames.data:
        output.write(frame)
    output.release()

def main() -> None:
    parser = init_argparse()
    args = parser.parse_args()
    read_file(args.file, args.output_file)

if __name__ == '__main__':
    main()