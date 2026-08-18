import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from dkit.utilities.job_tracker import (
    JobTracker,
    MultiProcessJobTracker,
    _BaseJobTracker,
)


def _key(entry):
    return entry["id"]


class TestJobTracker(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.file_name = str(Path(self.tmp_dir.name) / "journal")
        self.tracker = JobTracker(key=_key, file_name=self.file_name)

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_complete_and_is_completed(self):
        self.tracker.complete({"id": 1})
        self.assertTrue(self.tracker.is_completed({"id": 1}))
        self.assertFalse(self.tracker.is_completed({"id": 2}))

    def test_iter_not_completed(self):
        entries = [{"id": i} for i in range(5)]
        self.tracker.complete(entries[0])
        self.tracker.complete(entries[2])
        remaining = list(self.tracker.iter_not_completed(entries))
        self.assertEqual(remaining, [entries[1], entries[3], entries[4]])

    def test_contains(self):
        self.tracker.complete({"id": 1})
        self.assertIn({"id": 1}, self.tracker)
        self.assertNotIn({"id": 2}, self.tracker)

    def test_len(self):
        self.tracker.complete({"id": 1})
        self.tracker.complete({"id": 2})
        self.assertEqual(len(self.tracker), 2)

    def test_persistence_across_instances(self):
        self.tracker.complete({"id": 1})
        del self.tracker

        second = JobTracker(key=_key, file_name=self.file_name)
        self.assertTrue(second.is_completed({"id": 1}))

    def test_non_string_key_coercion(self):
        # regression: id is an int, gdbm requires string keys
        self.tracker.complete({"id": 42})
        self.assertTrue(self.tracker.is_completed({"id": 42}))

    def test_track_completes_on_success(self):
        with self.tracker.track({"id": 1}):
            pass
        self.assertTrue(self.tracker.is_completed({"id": 1}))

    def test_track_leaves_incomplete_on_exception(self):
        with self.assertRaises(ValueError):
            with self.tracker.track({"id": 1}):
                raise ValueError("boom")
        self.assertFalse(self.tracker.is_completed({"id": 1}))


class TestMultiProcessJobTracker(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.file_name = str(Path(self.tmp_dir.name) / "journal")
        self.tracker = MultiProcessJobTracker(
            key=_key, file_name=self.file_name
        )

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_complete_and_is_completed(self):
        self.tracker.complete({"id": 1})
        self.assertTrue(self.tracker.is_completed({"id": 1}))
        self.assertFalse(self.tracker.is_completed({"id": 2}))

    def test_concurrent_processes(self):
        # regression: gdbm refuses a second concurrent open of the
        # same file, so each process must open/close per operation
        repo_root = str(Path(__file__).resolve().parent.parent)
        script = f"""
import sys
sys.path.insert(0, {repo_root!r})
from dkit.utilities.job_tracker import MultiProcessJobTracker

tracker = MultiProcessJobTracker(
    key=lambda x: x["id"], file_name={self.file_name!r}
)
tracker.complete({{"id": sys.argv[1]}})
"""
        script_path = Path(self.tmp_dir.name) / "worker.py"
        script_path.write_text(script)

        procs = [
            subprocess.run(
                [sys.executable, str(script_path), str(i)],
                capture_output=True,
                text=True,
            )
            for i in range(3)
        ]
        for proc in procs:
            self.assertEqual(proc.returncode, 0, proc.stderr)

        for i in range(3):
            self.assertTrue(self.tracker.is_completed({"id": str(i)}))
        self.assertEqual(len(self.tracker), 3)


class TestAbstractBase(unittest.TestCase):
    def test_cannot_instantiate_base(self):
        with self.assertRaises(TypeError):
            _BaseJobTracker(key=_key)


if __name__ == "__main__":
    unittest.main()
