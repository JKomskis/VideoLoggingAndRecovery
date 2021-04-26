from __future__ import annotations
import pickle

from typing import Dict

class ObjectUpdateArguments():
    def __init__(self, function_name: str, start_frame: int = None, end_frame: int = None, **kwargs):
        self._function_name = function_name
        self._start_frame = start_frame
        self._end_frame = end_frame
        self._kwargs = kwargs
    
    @property
    def function_name(self) -> str:
        return self._function_name
    
    @property
    def start_frame(self) -> int:
        return self._start_frame
    
    @property
    def end_frame(self) -> int:
        return self._end_frame
    
    @property
    def kwargs(self) -> Dict:
        return self._kwargs
    
    def __repr__(self) -> str:
        return f'ObjectUpdateArguments({self.function_name!r}, {self.start_frame!r}, {self.end_frame!r}, {self.kwargs!r})'
    
    def __str__(self) -> str:
        return f'ObjectUpdateArguments({self.function_name}, {self.start_frame}, {self.end_frame}, {self.kwargs})'
    
    def __eq__(self, other) -> bool:
        return isinstance(other, ObjectUpdateArguments) \
            and self.function_name == other.function_name \
            and self.start_frame == other.start_frame \
            and self.end_frame == other.end_frame \
            and self.kwargs == other.kwargs
    
    def serialize(self) -> bytes:
        data = {
            'function_name': self.function_name,
            'start_frame': self.start_frame,
            'end_frame': self.end_frame,
            **self.kwargs
        }
        return pickle.dumps(data)
    
    @classmethod
    def deserialize(cls, data) -> ObjectUpdateArguments:
        data_dict = pickle.loads(data)
        function_name = data_dict['function_name']
        del data_dict['function_name']
        start_frame = data_dict['start_frame']
        del data_dict['start_frame']
        end_frame = data_dict['end_frame']
        del data_dict['end_frame']
        return ObjectUpdateArguments(function_name,
                                    start_frame,
                                    end_frame,
                                    **data_dict)