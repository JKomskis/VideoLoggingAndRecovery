import unittest

from src.pressure_point.pressure_point import PressurePoint, PressurePointLocation, PressurePointBehavior
from src.pressure_point.pressure_point_manager import PressurePointManager

class PressurePointManagerTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def test_pressure_point_manager_add(self):
        pressure_point = PressurePoint(PressurePointLocation.PETASTORE_STORAGE_ENGINE_DURING_WRITE,
                                PressurePointBehavior.EXCEPTION_AT_BEGINNING_OF_WRITE)

        pressure_point_mgr = PressurePointManager()
        pressure_point_mgr.reset()

        self.assertFalse(pressure_point_mgr.has_pressure_point(pressure_point))
        pressure_point_mgr.add_pressure_point(pressure_point)
        self.assertTrue(pressure_point_mgr.has_pressure_point(pressure_point))

    def test_pressure_point_manager_remove(self):
        pressure_point = PressurePoint(PressurePointLocation.PETASTORE_STORAGE_ENGINE_DURING_WRITE,
                                PressurePointBehavior.EXCEPTION_AT_BEGINNING_OF_WRITE)

        pressure_point_mgr = PressurePointManager()
        pressure_point_mgr.reset()

        self.assertFalse(pressure_point_mgr.has_pressure_point(pressure_point))
        pressure_point_mgr.add_pressure_point(pressure_point)
        self.assertTrue(pressure_point_mgr.has_pressure_point(pressure_point))
        pressure_point_mgr.remove_pressure_point(pressure_point)
        self.assertFalse(pressure_point_mgr.has_pressure_point(pressure_point))

    def test_pressure_point_manager_double_add(self):
        pressure_point = PressurePoint(PressurePointLocation.PETASTORE_STORAGE_ENGINE_DURING_WRITE,
                                PressurePointBehavior.EXCEPTION_AT_BEGINNING_OF_WRITE)

        pressure_point_mgr = PressurePointManager()
        pressure_point_mgr.reset()

        pressure_point_mgr.add_pressure_point(pressure_point)
        pressure_point_mgr.add_pressure_point(pressure_point)
        self.assertTrue(pressure_point_mgr.has_pressure_point(pressure_point))
        self.assertEqual(pressure_point_mgr.number_pressure_points_active(), 1)

    def test_pressure_point_manager_double_remove(self):
        pressure_point = PressurePoint(PressurePointLocation.PETASTORE_STORAGE_ENGINE_DURING_WRITE,
                                PressurePointBehavior.EXCEPTION_AT_BEGINNING_OF_WRITE)

        pressure_point_mgr = PressurePointManager()
        pressure_point_mgr.reset()

        self.assertFalse(pressure_point_mgr.has_pressure_point(pressure_point))
        pressure_point_mgr.add_pressure_point(pressure_point)
        self.assertTrue(pressure_point_mgr.has_pressure_point(pressure_point))
        pressure_point_mgr.remove_pressure_point(pressure_point)
        pressure_point_mgr.remove_pressure_point(pressure_point)
        self.assertFalse(pressure_point_mgr.has_pressure_point(pressure_point))
        

if __name__ == '__main__':
    unittest.main()