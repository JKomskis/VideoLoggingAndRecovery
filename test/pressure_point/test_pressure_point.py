import unittest

from src.pressure_point.pressure_point import PressurePoint, PressurePointLocation, PressurePointBehavior

class PressurePointTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def test_pressure_point_properties(self):
        pressure_point = PressurePoint(PressurePointLocation.PETASTORE_STORAGE_ENGINE_DURING_WRITE,
                                        PressurePointBehavior.EXCEPTION_AT_BEGINNING_OF_WRITE)
        
        self.assertEqual(pressure_point.location, PressurePointLocation.PETASTORE_STORAGE_ENGINE_DURING_WRITE)
        self.assertEqual(pressure_point.behavior, PressurePointBehavior.EXCEPTION_AT_BEGINNING_OF_WRITE)
        self.assertNotEqual(pressure_point.behavior, PressurePointBehavior.EXCPETION_DURING_WRITE)
    
    def test_pressure_point_equality(self):
        pressure_point = PressurePoint(PressurePointLocation.PETASTORE_STORAGE_ENGINE_DURING_WRITE,
                                        PressurePointBehavior.EXCEPTION_AT_BEGINNING_OF_WRITE)
        
        pressure_point_2 = PressurePoint(PressurePointLocation.PETASTORE_STORAGE_ENGINE_DURING_WRITE,
                                        PressurePointBehavior.EXCEPTION_AT_BEGINNING_OF_WRITE)
        
        pressure_point_3 = PressurePoint(PressurePointLocation.PETASTORE_STORAGE_ENGINE_DURING_WRITE,
                                        PressurePointBehavior.EXCPETION_DURING_WRITE)
        
        pressure_point_4 = PressurePoint(PressurePointLocation.UNKNOWN,
                                        PressurePointBehavior.EXCEPTION_AT_BEGINNING_OF_WRITE)
        
        self.assertEqual(pressure_point, pressure_point_2)
        self.assertNotEqual(pressure_point, pressure_point_3)
        self.assertNotEqual(pressure_point, pressure_point_4)
        
        

if __name__ == '__main__':
    unittest.main()