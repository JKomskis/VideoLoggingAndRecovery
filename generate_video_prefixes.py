import argparse
import cv2
from src.config.constants import INPUT_VIDEO_FOLDER

def init_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        usage='%(prog)s FILE',
        description="Generate a variety of prfixes of a given video for testing purposes."
    )
    parser.add_argument('file')
    return parser

def generate_prefixes(file_name: str) -> None:
    # Video prefix lengths, in minutes
    # video_prefix_lengths_min = [0.5, 1, 5, 10, 15, 20, 25, 30]
    video_prefix_lengths_min = [0.5, 1]
    video_prefix_lengths_sec = [int(i * 60) for i in video_prefix_lengths_min]

    video_name = 'traffic001'
    video_file = f'{INPUT_VIDEO_FOLDER}/{video_name}.mp4'

    video = cv2.VideoCapture(video_file)
    width = int(video.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(video.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = int(video.get(cv2.CAP_PROP_FPS))
    # fourcc = int(video.get(cv2.CAP_PROP_FOURCC))
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')

    output_video_files = [cv2.VideoWriter(f'{INPUT_VIDEO_FOLDER}/{video_name}_{length}.mp4', fourcc, fps, (width, height)) \
        for length in video_prefix_lengths_sec]

    video_prefix_lengths_frames = [int(i * 60 * fps) for i in video_prefix_lengths_min]

    _, frame = video.read()
    frame_num = 0
    # print(f'FRAME: {frame_id} {frame}')

    while frame is not None:
        if frame_num % 100 == 0:
            print(f'Frame: {frame_num}')
        for i, writer in enumerate(output_video_files):
            if frame_num < video_prefix_lengths_frames[i]:
                writer.write(frame)
        
        _, frame = video.read()
        frame_num += 1
        if frame_num > max(video_prefix_lengths_frames):
            break

    video.release()
    for writer in output_video_files:
        writer.release()

def main() -> None:
    parser = init_argparse()
    args = parser.parse_args()
    generate_prefixes(args.file)

if __name__ == '__main__':
    main()