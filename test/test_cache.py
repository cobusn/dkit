import unittest
import sys
sys.path.insert(0, "..") # noqa
from dkit.utilities.cache import ObjectFileCache


class TestCache(unittest.TestCase):

    def setUp(self):
        self.cache = ObjectFileCache("data/test.cache")

    def test_set_get(self):
        my_key_object = {1: 1, 2: 2}
        self.cache.set_item(my_key_object, my_key_object)
        retrieved_data = self.cache.get_item(my_key_object)
        self.assertEqual(retrieved_data, my_key_object)


if __name__ == '__main__':
    unittest.main()
