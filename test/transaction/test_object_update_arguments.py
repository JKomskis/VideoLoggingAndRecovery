import unittest

from src.transaction.object_update_arguments import ObjectUpdateArguments
import cv2

class ObjectUpdateArgumentsTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def test_should_be_equal(self):
        update_operation = ObjectUpdateArguments('resize', 0, 300, dsize=(480, 270), interpolation=cv2.INTER_AREA)
        update_operation_2 = ObjectUpdateArguments('resize', 0, 300, dsize=(480, 270), interpolation=cv2.INTER_AREA)

        self.assertEqual(update_operation, update_operation_2)
    
    def test_should_not_be_equal(self):
        update_operation = ObjectUpdateArguments('resize', 0, 300, dsize=(480, 270), interpolation=cv2.INTER_AREA)
        
        update_operation_2 = ObjectUpdateArguments('test_filter', 0, 300, dsize=(480, 270), interpolation=cv2.INTER_AREA)
        self.assertNotEqual(update_operation, update_operation_2)

        update_operation_2 = ObjectUpdateArguments('resize', 1, 300, dsize=(480, 270), interpolation=cv2.INTER_AREA)
        self.assertNotEqual(update_operation, update_operation_2)

        update_operation_2 = ObjectUpdateArguments('resize', 0, 299, dsize=(480, 270), interpolation=cv2.INTER_AREA)
        self.assertNotEqual(update_operation, update_operation_2)

        update_operation_2 = ObjectUpdateArguments('resize', 0, 300, dsize=(480, 271), interpolation=cv2.INTER_AREA)
        self.assertNotEqual(update_operation, update_operation_2)

    def test_should_serialize_deserialize_object(self):
        update_operation = ObjectUpdateArguments('resize', 0, 300, dsize=(480, 270), interpolation=cv2.INTER_AREA)
        data = update_operation.serialize()

        deserialized_update = ObjectUpdateArguments.deserialize(data)

        self.assertEqual(update_operation, deserialized_update)


if __name__ == '__main__':
    unittest.main()     