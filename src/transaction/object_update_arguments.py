class ObjectUpdateArguments():
    def __init__(self, function_name: str, start_frame: int = None, end_frame: int = None, **kwargs):
        self._function_name = function_name
        self._start_frame = start_frame
        self._end_frame = end_frame
        self._kwargs = kwargs
    
    @property
    def function_name(self):
        return self._function_name
    
    @property
    def start_frame(self):
        return self._start_frame
    
    @property
    def end_frame(self):
        return self._end_frame
    
    @property
    def kwargs(self):
        return self._kwargs