import cv2
from typing import Iterator, Dict

from src.readers.abstract_reader import AbstractReader
from src.utils.logging_manager import LoggingLevel
from src.utils.logging_manager import LoggingManager


class OpenCVReader(AbstractReader):

    def __init__(self, *args, start_frame_id=0, **kwargs):
        """
            Reads video using OpenCV and yields frame data.
            It will use the `start_frame_id` while annotating the
            frames. The first frame will be annotated with `start_frame_id`
            Attributes:
                start_frame_id (int): id assigned to first read frame
                    eg: start_frame_id=10, returned Iterator will be
                    [{10, frame1}, {11, frame2} ...]
                    It is different from offset. Offset defines where in video
                    should we start reading. And start_frame_id defines the id
                    we assign to first read frame.
         """
        self._start_frame_id = start_frame_id
        super().__init__(*args, **kwargs)

    def _read(self) -> Iterator[Dict]:
        video = cv2.VideoCapture(self.file_url)
        video_offset = self.offset if self.offset else 0
        video.set(cv2.CAP_PROP_POS_FRAMES, video_offset)

        LoggingManager().log("Reading frames", LoggingLevel.INFO)

        _, frame = video.read()
        frame_id = self._start_frame_id
        # print(f'FRAME: {frame_id} {frame}')

        while frame is not None:
            yield {'id': frame_id, 'data': frame}
            _, frame = video.read()
            frame_id += 1
    
    def video_width(self) -> int:
        video = cv2.VideoCapture(self.file_url)

        if video.isOpened():
            return int(video.get(cv2.CAP_PROP_FRAME_WIDTH))
        return -1
    
    def video_height(self) -> int:
        video = cv2.VideoCapture(self.file_url)

        if video.isOpened():
            return int(video.get(cv2.CAP_PROP_FRAME_HEIGHT))
        return -1