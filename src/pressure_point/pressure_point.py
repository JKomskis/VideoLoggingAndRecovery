from enum import Enum

class PressurePointLocation(Enum):
    PETASTORE_STORAGE_ENGINE_DURING_WRITE = 1
    UNKNOWN = 2

class PressurePointBehavior(Enum):
    EXCEPTION_AT_BEGINNING_OF_WRITE = 1
    EXCPETION_DURING_WRITE = 2

class PressurePoint():
    def __init__(self, location: PressurePointLocation, behavior: PressurePointBehavior):
        self._location = location
        self._behavior = behavior
    
    @property
    def location(self):
        return self._location
    
    @property
    def behavior(self):
        return self._behavior
    
    def __eq__(self, obj):
        return isinstance(obj, PressurePoint) \
                and obj.location == self.location \
                and obj.behavior == self.behavior
    
    def __str__(self):
        return f'PressurePoint{{Location: {self.location}, Behavior: {self.behavior}}}'
    
    def __key(self):
        return (self.location, self.behavior)
    
    def __hash__(self):
        return hash(self.__key())