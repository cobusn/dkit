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
import filelock
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
        self._tmpdir = tempfile.mkdtemp(prefix="jsondb_")
        self.db = JSONDB(self._tmpdir)

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
                dbz = JSONDB(tmpdir, compress=c)
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
            _ = JSONDB("data/jsondb", "wrong")

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
        db = JSONDB(self._tmpdir, allow_null=False)
        with self.assertRaises(ValueError):
            db["null"] = None

    def test_10_iter_keys_for_illegal_chars(self):
        """test iteration yields the sanitised key (no index to
        recover the original un-sanitised key from)"""
        bad = "bad!key&"
        self.db[bad] = True
        keys = list(self.db)
        self.assertIn(self.db._transform(bad), keys)

    def test_11_created_after_filters(self):
        """test created_after filtering"""
        before = datetime.now()
        self._populate(self.db)
        time.sleep(0.01)
        after = datetime.now()
        filtered = JSONDB(self._tmpdir, created_after=after)
        self.assertEqual(dict(filtered.items()), {})
        filtered2 = JSONDB(self._tmpdir, created_after=before - timedelta(seconds=1))
        self.assertEqual(dict(filtered2.items()), data)

    def test_12_lock_prevents_concurrent_check_then_write(self):
        """test lock() blocks a second holder for the same key"""
        key = "1"
        with self.db.lock(key):
            other = JSONDB(self._tmpdir)
            other_lock = other.lock(key)
            with self.assertRaises(filelock.Timeout):
                other_lock.acquire(timeout=0.1)

    def test_13_no_leftover_tmp_file_after_write(self):
        """test atomic write leaves no temporary file behind"""
        self.db["1"] = 1
        tmp_files = list(Path(self._tmpdir).glob("*.tmp"))
        self.assertEqual(tmp_files, [])

    def test_14_lock_files_kept_out_of_main_directory(self):
        """test lock files are written under a .locks subdirectory"""
        with self.db.lock("1"):
            pass
        main_dir_locks = list(Path(self._tmpdir).glob("*.lock"))
        self.assertEqual(main_dir_locks, [])
        locks_subdir = list((Path(self._tmpdir) / ".locks").glob("*.lock"))
        self.assertEqual(len(locks_subdir), 1)

    def test_15_purge_locks_removes_unheld_locks(self):
        """test purge_locks removes locks that are not held"""
        with self.db.lock("1"):
            pass
        self.db.purge_locks()
        locks_subdir = list((Path(self._tmpdir) / ".locks").glob("*.lock"))
        self.assertEqual(locks_subdir, [])

    def test_16_purge_locks_keeps_held_locks(self):
        """test purge_locks does not remove a lock currently held"""
        with self.db.lock("1"):
            self.db.purge_locks()
            locks_subdir = list((Path(self._tmpdir) / ".locks").glob("*.lock"))
            self.assertEqual(len(locks_subdir), 1)

    def test_17_performance_1000(self):
        """benchmark add/read 1000 entries (no assertions on timing)"""
        db = JSONDB(self._tmpdir)
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
