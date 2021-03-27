import cv2
import numpy as np

from src.transaction.object_update_arguments import ObjectUpdateArguments

class OpenCVUpdateProcessor():
    def __init__(self):
        self.function_map = {
            'grayscale': self._grayscale,
            'gaussian_blur': self._gaussian_blur,
            'resize': self._resize
        }
        pass

    def apply(self, source_frame, object_update_arguments: ObjectUpdateArguments):
        function = self.function_map[object_update_arguments.function_name]
        return function(source_frame, object_update_arguments)

    def _grayscale(self, source_frame, object_update_arguments: ObjectUpdateArguments):
        return cv2.cvtColor(cv2.cvtColor(source_frame, cv2.COLOR_BGR2GRAY), cv2.COLOR_GRAY2BGR)
    
    def _gaussian_blur(self, source_frame, object_update_arguments: ObjectUpdateArguments):
        return cv2.GaussianBlur(source_frame, **object_update_arguments.kwargs)
    
    def _resize(self, source_frame, object_update_arguments: ObjectUpdateArguments):
        return cv2.resize(source_frame, **object_update_arguments.kwargs)