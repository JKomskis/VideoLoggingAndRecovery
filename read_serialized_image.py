import pandas as pd
import cv2
import glob
import argparse

from src.config.constants import \
    INPUT_VIDEO_FOLDER, \
    TRANSACTION_STORAGE_FOLDER

def init_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        usage='%(prog)s FILE OUTPUT_FILE',
        description="Read file from transaction storage folder to a video file for testing purposes."
    )
    parser.add_argument('file')
    parser.add_argument('output_file')
    return parser

def read_serialized_image(input_file_path: str, output_file_path: str) -> None:
    frames = None

    for name in glob.glob(f'{input_file_path}_*'):
        batch = pd.read_pickle(name)
        if frames is None:
            frames = batch
        else:
            frames = frames.append(batch)

    print(frames.columns)
    print(frames.size)
    frames = frames.sort_values(by='id')

    width = frames.data.iloc[0].shape[1]
    height = frames.data.iloc[0].shape[0]
    print(f'({width}, {height})')
    fps = 30
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    output = cv2.VideoWriter(output_file_path, fourcc, fps, (width, height))

    for frame in frames.data:
        output.write(frame)
    output.release()

def main() -> None:
    parser = init_argparse()
    args = parser.parse_args()
    read_serialized_image(args.file, args.output_file)

if __name__ == '__main__':
    main()