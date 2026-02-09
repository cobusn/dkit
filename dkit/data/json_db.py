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
import sys
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path

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


class JSONDB(Mapping):
    """JSON file-based DB

    File-based database that maps keys to JSON files on disk.

    This class is useful for keeping track of work completed in a
    multi-processing environment.

    args:
        - path: file path to folder used to store json files
        - suffix: file suffix for json files (e.g. json)
        - compress: compress files. provide the compression library:
            - bz2
            - gz
            - zstd
        - created_after: only return values for files modified after this
          datetime (uses file mtime)
        - allow_null: will not store Null values if enabled. Useful for
          processes where null means a failure (and raise a ValueError)

    throws:
        - TypeError if the key is not of type str
        - ValueError if value is None and allow_null is False
    """
    def __init__(self, path: str, compress=None, allow_null: bool = True,
                 created_after: datetime = None):
        """create a JSONDB instance"""
        self.path: Path = Path(path)
        self.allow_null = allow_null
        self.created_after = created_after
        self.path.mkdir(parents=True, exist_ok=True)
        self._index_path = self.path / ".index.json"
        self._index_cache = None
        self._index_mtime = None  # mtime used to validate index cache
        self._mtime_cache = {}
        if compress is None:
            self.file_io = FileObjStub
            self.suffix = "json"
        else:
            if compress not in C_OPTIONS:
                raise ValueError(f"compress should be one of {', '.join(C_OPTIONS.keys())}")
            self.file_io = C_OPTIONS[compress]
            self.suffix = f"json.{compress}"
        # Lazy rebuild: index is populated on key access, not by scanning files

    def _file_path(self, key):
        """build the file path for a transformed key"""
        return self.path / f"{key}.{self.suffix}"

    def _reverse_transform(self, filename: str):
        """translate filename back to key"""
        name = Path(filename).name
        return name.removesuffix(f".{self.suffix}")

    def _transform(self, key):
        """override this if your keys have special characters that cannot be used
        in filenames
        """
        return sanitise_name(key)

    def _get_mtime(self, safe_key: str):
        """get cached mtime for a safe key"""
        cached = self._mtime_cache.get(safe_key)
        if cached is not None:
            return cached
        fp = self._file_path(safe_key)
        try:
            mtime = fp.stat().st_mtime
        except FileNotFoundError:
            return None
        self._mtime_cache[safe_key] = mtime
        return mtime

    def _passes_created_after(self, fp: Path):
        """check if file mtime is after created_after"""
        if self.created_after is None:
            return True
        safe_key = self._reverse_transform(str(fp))
        mtime = self._get_mtime(safe_key)
        if mtime is None:
            return False
        return mtime > self.created_after.timestamp()

    def _load_index(self):
        """load index from disk with mtime cache"""
        if not self._index_path.exists():
            self._index_cache = {}
            self._index_mtime = None
            return {}
        try:
            mtime = self._index_path.stat().st_mtime
            if self._index_cache is not None and mtime == self._index_mtime:
                return self._index_cache
        except FileNotFoundError:
            self._index_cache = {}
            self._index_mtime = None
            return {}
        try:
            with self._index_path.open("rt") as infile:
                index = json.load(infile)
                self._index_cache = index
                self._index_mtime = mtime
                return index
        except (json.JSONDecodeError, OSError):
            self._index_cache = {}
            self._index_mtime = None
            return {}

    def _save_index(self, index):
        """atomically write index to disk and refresh cache"""
        if not index:
            try:
                self._index_path.unlink()
            except FileNotFoundError:
                pass
            self._index_cache = {}
            self._index_mtime = None
            return
        tmp_path = self._index_path.with_suffix(self._index_path.suffix + ".tmp")
        with tmp_path.open("wt") as outfile:
            json.dump(index, outfile)
        tmp_path.replace(self._index_path)
        self._index_cache = index
        try:
            self._index_mtime = self._index_path.stat().st_mtime
        except FileNotFoundError:
            self._index_mtime = None

    def _has_index(self):
        """return True if the index file exists"""
        return self._index_path.exists()

    def _rebuild_index(self):
        """rebuild index from existing data files"""
        index = {}
        for item in self.path.glob(f"*.{self.suffix}"):
            if item == self._index_path:
                continue
            safe_key = self._reverse_transform(str(item))
            index[safe_key] = safe_key
        self._save_index(index)
        return index

    def refresh(self):
        """refresh cached index and mtimes from disk"""
        self._index_cache = None
        self._index_mtime = None
        self._mtime_cache = {}
        self._load_index()

    def append(self, key, value):
        """store a value under key"""
        if not isinstance(key, str):
            raise TypeError("key should be of type str")
        if value is None and self.allow_null is False:
            raise ValueError(
                f"'{key}' has Null value and allow_null is False"
            )
        else:
            safe_key = self._transform(key)
            fname = self._file_path(safe_key)
            if fname == self._index_path:
                raise ValueError(f"'{key}' is a reserved key")
            with self.file_io.open(fname, "wt") as outfile:
                json.dump(value, outfile)
            index = self._load_index()
            index[key] = safe_key
            self._save_index(index)
            mtime = self._get_mtime(safe_key)
            if mtime is not None:
                self._mtime_cache[safe_key] = mtime

    def __setitem__(self, key, value):
        """set a value by key"""
        self.append(key, value)

    def __contains__(self, key):
        """return True if key exists and passes created_after"""
        index = self._load_index()
        safe_key = index.get(key)
        if safe_key is None:
            safe_key = self._transform(key)
        fp = self._file_path(safe_key)
        if fp == self._index_path:
            return False
        if fp.exists():
            if key not in index:
                index[key] = safe_key
                self._save_index(index)
            return self._passes_created_after(fp)
        return False

    def __len__(self):
        """return count of stored items (respects created_after)"""
        index = self._load_index()
        if self.created_after is None:
            return len(index)
        return sum(
            1 for safe_key in index.values()
            if self._passes_created_after(self._file_path(safe_key))
        )

    def __iter__(self):
        """iterate over keys (respects created_after)"""
        index = self._load_index()
        for key, safe_key in index.items():
            if self._passes_created_after(self._file_path(safe_key)):
                yield key

    def __delitem__(self, key):
        """delete an item by key"""
        index = self._load_index()
        safe_key = index.get(key, self._transform(key))
        fp = self._file_path(safe_key)
        try:
            fp.unlink()
        except FileNotFoundError:
            raise KeyError(key)
        if key in index:
            del index[key]
            self._save_index(index)
        if safe_key in self._mtime_cache:
            del self._mtime_cache[safe_key]

    def __getitem__(self, key):
        """get a value by key (respects created_after)"""
        index = self._load_index()
        safe_key = index.get(key)
        if safe_key is None:
            safe_key = self._transform(key)
        fp = self._file_path(safe_key)
        if fp == self._index_path:
            raise KeyError(key)
        try:
            if key not in index and fp.exists():
                index[key] = safe_key
                self._save_index(index)
            if not self._passes_created_after(fp):
                raise KeyError(key)
            with self.file_io.open(fp, "rt") as infile:
                return json.load(infile)
        except FileNotFoundError:
            raise KeyError(key)
