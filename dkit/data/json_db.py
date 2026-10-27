# Copyright (c) 2025 Cobus Nel
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
"""
Dict-like file database for JSON files where each JSON file is mapped to a key.
"""
import bz2
import gzip
import json
import os
import sys
import uuid
from collections.abc import MutableMapping
from datetime import datetime
from pathlib import Path

import filelock

from ..utilities.file_helper import FileObjStub, sanitise_name


if sys.version_info >= (3, 14):
    from compression import zstd
else:
    import zstandard as zstd


C_OPTIONS = {
    "bz2": bz2,
    "gz": gzip,
    "zstd": zstd
}


class JSONDB(MutableMapping):
    """JSON file-based DB

    File-based database that maps keys to JSON files on disk.

    This class is useful for keeping track of work completed in a
    multi-processing environment, e.g. so a restarted ETL job can skip
    keys that were already processed.

    This version does not maintain an index (keys are enumerated by
    listing the folder) and can be used in multiple processes. Writes
    are atomic (written to a temporary file then renamed) so readers
    never observe a partially written file. Use `lock()` to make a
    check-then-write sequence (e.g. "skip if already done") atomic
    across processes.

    Keys with characters not allowed in filenames are sanitised before
    being stored. There is no index mapping the sanitised on-disk name
    back to the original key, so iterating over the database (or
    `len()`) yields the sanitised form, not the original key.

    `lock()` leaves a `<key>.<suffix>.lock` file behind in a `.locks`
    subdirectory after use (filelock does not delete it on release).
    This is expected and does not affect `len()`/iteration, which only
    look at the main directory. Call `purge_locks()` between batches
    to remove lock files that are not currently held.

    args:
        - path: file path to folder used to store json files
        - suffix: file suffix for json files (e.g. json)
        - compress: compress files. provide the compression library:
            - bz2
            - gz
            - zstd
        - allow_null: will not store Null values if enabled.  Useful for
        processes where null means a failure (and raise a ValueError)
        - created_after: only consider entries whose file was last
          modified after this datetime (uses file mtime)

    throws:
        - TypeError if the key is not of type str
        - ValueError if value is None and allow_null is False
    """
    def __init__(self, path: str, compress=None, allow_null: bool = True,
                 created_after: datetime = None):
        self.path: Path = Path(path)
        self.allow_null = allow_null
        self.created_after = created_after
        if compress is None:
            self.file_io = FileObjStub
            self.suffix = "json"
        else:
            if compress not in C_OPTIONS:
                raise ValueError(f"compress should be one of {', '.join(C_OPTIONS.keys())}")
            self.file_io = C_OPTIONS[compress]
            self.suffix = f"json.{compress}"
        self.path.mkdir(parents=True, exist_ok=True)

    def _file_path(self, key):
        return self.path / f"{key}.{self.suffix}"

    def _reverse_transform(self, filename: str):
        """translate filname back to key"""
        key = filename.removesuffix(f".{self.suffix}")
        return key.removeprefix(f"{self.path}/")

    def _transform(self, key):
        """overide this if your keys have special characters
        that cannot be used in filenames
        """
        return sanitise_name(key)

    def _passes_created_after(self, fp: Path):
        """check if file mtime is after created_after"""
        if self.created_after is None:
            return True
        try:
            mtime = fp.stat().st_mtime
        except FileNotFoundError:
            return False
        return mtime > self.created_after.timestamp()

    def _locks_dir(self) -> Path:
        """directory holding filelock marker files"""
        return self.path / ".locks"

    def lock(self, key):
        """return a filelock.FileLock for the given key

        Hold this lock around a check-then-write sequence (e.g. "skip
        if already done") to make it atomic across processes:

            with db.lock(key):
                if key not in db:
                    db[key] = expensive_compute()
        """
        locks_dir = self._locks_dir()
        locks_dir.mkdir(parents=True, exist_ok=True)
        safe_key = self._transform(key)
        return filelock.FileLock(str(locks_dir / f"{safe_key}.{self.suffix}.lock"))

    def purge_locks(self):
        """remove lock files that are not currently held

        Only safe to call when no other process is expected to
        request a new lock during the purge, e.g. between ETL
        batches.
        """
        locks_dir = self._locks_dir()
        if not locks_dir.exists():
            return
        for fp in locks_dir.glob("*.lock"):
            lock = filelock.FileLock(str(fp))
            try:
                lock.acquire(timeout=0)
            except filelock.Timeout:
                continue
            try:
                fp.unlink()
            except FileNotFoundError:
                pass
            finally:
                lock.release()

    def append(self, key, value):
        if value is None and self.allow_null is False:
            raise ValueError(
                f"'{key}' has Null value and allow_null is False"
            )
        fname = self._file_path(key)
        tmp_name = fname.with_name(f"{fname.name}.{uuid.uuid4().hex}.tmp")
        with self.file_io.open(tmp_name, "wt") as outfile:
            json.dump(value, outfile)
        os.replace(tmp_name, fname)

    def __setitem__(self, key, value):
        if not isinstance(key, str):
            raise TypeError("key should be of type str")
        if value is None and self.allow_null is False:
            raise ValueError(
                f"'{key}' has Null value and allow_null is False"
            )
        self.append(self._transform(key), value)

    def __contains__(self, key):
        fp = self._file_path(self._transform(key))
        try:
            mtime = fp.stat().st_mtime
        except FileNotFoundError:
            return False
        if self.created_after is None:
            return True
        return mtime > self.created_after.timestamp()

    def __len__(self):
        return sum(
            1 for item in self.path.glob(f"*.{self.suffix}")
            if self._passes_created_after(item)
        )

    def __iter__(self):
        for item in self.path.glob(f"*.{self.suffix}"):
            if self._passes_created_after(item):
                yield self._reverse_transform(str(item))

    def __delitem__(self, key):
        fp = self._file_path(self._transform(key))
        try:
            fp.unlink()
        except FileNotFoundError:
            raise KeyError(key)

    def __getitem__(self, key):
        fp = self._file_path(self._transform(key))
        if not self._passes_created_after(fp):
            raise KeyError(key)
        try:
            with self.file_io.open(fp) as infile:
                return json.load(infile)
        except FileNotFoundError:
            raise KeyError(key)
