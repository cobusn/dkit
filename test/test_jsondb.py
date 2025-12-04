import unittest
import sys
import bz2
sys.path.insert(0, "..") # noqa
# from dkit.utilities.cache import ObjectFileCache
from dkit.data.json_db import JSONDB
# from datetime import datetime


data = {
    "1": 1,
    "2": 2,
    "string": "string",
}


class TestJSONDB(unittest.TestCase):

    def _populate(self, db):
        for k, v in data.items():
            db[k] = v

    def _delete_all(self, db):
        for k in data:
            del db[k]

    def setUp(self):
        self.db = JSONDB("data/jsondb")

    def test_1_set_data(self):
        """test setting data"""
        self._populate(self.db)

    def test_2_read_data(self):
        """test reading data"""
        local = dict(self.db.items())
        self.assertEqual(local, data)

    def test_3_del_data(self):
        """test reading data"""
        self._delete_all(self.db)
        self.assertEqual({}, self.db)

    def test_4_compress(self):
        for c in ["gz", "bz2"]:
            dbz = JSONDB("data/jsondb", compress=c)
            self._populate(dbz)
            # self._delete_all(dbz)

    def test_5_raises_wrong_key(self):
        """test raise on wrong key type"""
        with self.assertRaises(TypeError):
            self.db[1] = 1

    def test_6_raises_wrong_compression(self):
        """test raise on wrong key type"""
        with self.assertRaises(ValueError):
            _ = JSONDB("data/jsondb", "wrong")

    def test_7_illegal_characters(self):
        """test with keys that have illegal characters"""
        bad = "bad!key&"
        self.db[bad] = True
        self.assertEqual(self.db[bad], True)
        self.assertTrue(bad in self.db)
        del self.db[bad]
        self.assertFalse(bad in self.db)

    """
    self.cache.set_item(my_key_object, my_key_object)
    retrieved_data = self.cache.get_item(my_key_object)
    self.assertEqual(retrieved_data, my_key_object)
    """


if __name__ == '__main__':
    unittest.main()
