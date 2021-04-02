from src.pressure_point.pressure_point import PressurePoint

class PressurePointManager():
    _instance = None
    _pressure_point_map = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(PressurePointManager, cls).__new__(cls)
            cls._instance._pressure_point_map = set()
        return cls._instance
    
    def add_pressure_point(self, pressure_point: PressurePoint) -> None:
        self._pressure_point_map.add(pressure_point)
    
    def remove_pressure_point(self, pressure_point: PressurePoint) -> None:
        if self.has_pressure_point(pressure_point):
            self._pressure_point_map.remove(pressure_point)
    
    def has_pressure_point(self, pressure_point) -> bool:
        return pressure_point in self._pressure_point_map
    
    def number_pressure_points_active(self) -> int:
        return len(self._pressure_point_map)
    
    def reset(self) -> None:
        self._pressure_point_map.clear()