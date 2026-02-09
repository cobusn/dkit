import unittest
import sys
import bz2
import shutil
import tempfile
import time
import timeit
from datetime import datetime, timedelta
from pathlib import Path
sys.path.insert(0, "..") # noqa
# from dkit.utilities.cache import ObjectFileCache
from dkit.data.json_db import JSONDB2
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
        self._tmpdir = tempfile.mkdtemp(prefix="jsondb_")
        self.db = JSONDB2(self._tmpdir)

    def tearDown(self):
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_1_set_data(self):
        """test setting data"""
        self._populate(self.db)
        self.assertEqual(dict(self.db.items()), data)

    def test_2_read_data(self):
        """test reading data"""
        self._populate(self.db)
        local = dict(self.db.items())
        self.assertEqual(local, data)

    def test_3_del_data(self):
        """test reading data"""
        self._populate(self.db)
        self._delete_all(self.db)
        self.assertEqual({}, dict(self.db.items()))
        self.assertEqual(len(list(Path(self._tmpdir).glob("*.json*"))), 0)

    def test_4_compress(self):
        for c in ["gz", "bz2"]:
            with tempfile.TemporaryDirectory(prefix=f"jsondb_{c}_") as tmpdir:
                dbz = JSONDB2(tmpdir, compress=c)
                self._populate(dbz)
                self.assertEqual(dict(dbz.items()), data)
                self._delete_all(dbz)
                self.assertEqual(len(list(Path(tmpdir).glob("*.json*"))), 0)

    def test_5_raises_wrong_key(self):
        """test raise on wrong key type"""
        with self.assertRaises(TypeError):
            self.db[1] = 1

    def test_6_raises_wrong_compression(self):
        """test raise on wrong key type"""
        with self.assertRaises(ValueError):
            _ = JSONDB2("data/jsondb", "wrong")

    def test_7_illegal_characters(self):
        """test with keys that have illegal characters"""
        bad = "bad!key&"
        self.db[bad] = True
        self.assertEqual(self.db[bad], True)
        self.assertTrue(bad in self.db)
        del self.db[bad]
        self.assertFalse(bad in self.db)

    def test_8_missing_key_raises(self):
        """test missing key behavior"""
        with self.assertRaises(KeyError):
            _ = self.db["missing"]
        with self.assertRaises(KeyError):
            del self.db["missing"]

    def test_9_disallow_null(self):
        """test disallowing null values"""
        db = JSONDB2(self._tmpdir, allow_null=False)
        with self.assertRaises(ValueError):
            db["null"] = None

    def test_10_iter_keys_for_illegal_chars(self):
        """test iteration keys for illegal characters"""
        bad = "bad!key&"
        self.db[bad] = True
        keys = list(self.db)
        self.assertIn(bad, keys)

    def test_11_created_after_filters(self):
        """test created_after filtering"""
        before = datetime.now()
        self._populate(self.db)
        time.sleep(0.01)
        after = datetime.now()
        filtered = JSONDB2(self._tmpdir, created_after=after)
        self.assertEqual(dict(filtered.items()), {})
        filtered2 = JSONDB2(self._tmpdir, created_after=before - timedelta(seconds=1))
        self.assertEqual(dict(filtered2.items()), data)

    def test_12_missing_index_populates_on_access(self):
        """test missing index is populated on access"""
        self._populate(self.db)
        index_path = Path(self._tmpdir) / ".index.json"
        if index_path.exists():
            index_path.unlink()
        db2 = JSONDB2(self._tmpdir)
        self.assertFalse(db2._has_index())
        _ = db2["1"]
        index = db2._load_index()
        self.assertIn("1", index)
        self.assertTrue(db2._has_index())

    def test_13_performance_1000(self):
        """benchmark add/read 1000 entries (no assertions on timing)"""
        db = JSONDB2(self._tmpdir)
        entries = {str(i): i for i in range(1000)}

        start = timeit.default_timer()
        for k, v in entries.items():
            db[k] = v
        add_elapsed = timeit.default_timer() - start

        start = timeit.default_timer()
        for k in entries:
            _ = db[k]
        read_elapsed = timeit.default_timer() - start

        print(f"JSONDB add 1000: {add_elapsed:.6f}s")
        print(f"JSONDB read 1000: {read_elapsed:.6f}s")

        self.assertTrue(add_elapsed >= 0)
        self.assertTrue(read_elapsed >= 0)

    """
    self.cache.set_item(my_key_object, my_key_object)
    retrieved_data = self.cache.get_item(my_key_object)
    self.assertEqual(retrieved_data, my_key_object)
    """


if __name__ == '__main__':
    unittest.main()
