import cv2
import numpy as np

from src.transaction.object_update_arguments import ObjectUpdateArguments

class UpdateNotReversibleException(Exception):
    def __init__(self, object_update_arguments):
        super(UpdateNotReversibleException, self).__init__(object_update_arguments)

class OpenCVUpdateProcessor():
    def __init__(self):
        self.function_map = {
            'grayscale': self._grayscale,
            'gaussian_blur': self._gaussian_blur,
            'resize': self._resize,
            'invert_color': self._invert_color,
            'contrast_brightness': self._contrast_brightness,
            'test_filter': self._test_filter
        }

        self.reversible_map = {
            'invert_color': self._reverse_invert_color,
        }
        pass

    def apply(self, source_frame, object_update_arguments: ObjectUpdateArguments):
        function = self.function_map[object_update_arguments.function_name]
        return function(source_frame, object_update_arguments)
    
    def is_reversible(self, object_update_arguments: ObjectUpdateArguments):
        return object_update_arguments.function_name in self.reversible_map
    
    def reverse(self, object_update_arguments: ObjectUpdateArguments):
        if not self.is_reversible(object_update_arguments):
            raise UpdateNotReversibleException(object_update_arguments)
        return self.reversible_map[object_update_arguments.function_name](object_update_arguments)

    def _grayscale(self, source_frame, object_update_arguments: ObjectUpdateArguments):
        return cv2.cvtColor(cv2.cvtColor(source_frame, cv2.COLOR_BGR2GRAY), cv2.COLOR_GRAY2BGR)
    
    def _gaussian_blur(self, source_frame, object_update_arguments: ObjectUpdateArguments):
        return cv2.GaussianBlur(source_frame, **object_update_arguments.kwargs)
    
    def _resize(self, source_frame, object_update_arguments: ObjectUpdateArguments):
        return cv2.resize(source_frame, **object_update_arguments.kwargs)
    
    def _invert_color(self, source_frame, object_update_arguments: ObjectUpdateArguments):
        return cv2.bitwise_not(source_frame, **object_update_arguments.kwargs)
    
    def _contrast_brightness(self, source_frame, object_update_arguments: ObjectUpdateArguments):
        return cv2.addWeighted(source_frame,
                                object_update_arguments.kwargs['contrast'],
                                source_frame,
                                0,
                                object_update_arguments.kwargs['brightness'])
    
    def _test_filter(self, source_frame, object_update_arguments: ObjectUpdateArguments):
        return np.full(source_frame.shape, 255, dtype=np.uint8)
    
    def _reverse_invert_color(self, object_update_arguments: ObjectUpdateArguments):
        # Applying the same update again will reverse the color inversion, no changes needed
        return ObjectUpdateArguments(object_update_arguments.function_name,
                                    object_update_arguments.start_frame,
                                    object_update_arguments.end_frame)