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
Dict like file database for json files where each json
file is mapped to a key.

This library do not cache data and file I/O occurs on each
transaction.

"""
from collections.abc import Mapping
from pathlib import Path
import json
from ..utilities.file_helper import FileObjStub, sanitise_name
import bz2
import gzip
import sys
if sys.version_info >= (3, 14):
    from compression import zstd
else:
    from backports import zstd


C_OPTIONS = {
    "bz2": bz2,
    "gz": gzip,
    "zstd": zstd
}


class JSONDB(Mapping):
    """json file based db

    )This is mainly useful for low IO data and keeping track of
    work completed in a multi-processing environment.

    args:
        - path: file path to folder used to store json files
        - suffix: file suffix for json files (e.g. json)
        - compress: compress files. provide the compression library:
            - bz2
            - gz
            - zstd
        - allow_null: will not store Null values if enabled.  Useful for
        processes where null means a failure (and raise a ValueError)

    throws:
        - TypeError if the key is not of type str
        - ValueError if value is None and allow_null is False
    """
    def __init__(self, path: str, compress=None, allow_null: bool = True):
        self.path: Path = Path(path)
        self.allow_null = allow_null
        if compress is None:
            self.file_io = FileObjStub
            self.suffix = "json"
        else:
            if compress not in C_OPTIONS:
                raise ValueError(f"compress should be one of {', '.join(C_OPTIONS.keys())}")
            self.file_io = C_OPTIONS[compress]
            self.suffix = f"json.{compress}"

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

    def append(self, key, value):
        if value is None and self.allow_null is False:
            raise ValueError(
                f"'{key}' has Null value and allow_null is False"
            )
        else:
            fname = self._file_path(key)
            with self.file_io.open(fname, "wt") as outfile:
                json.dump(value, outfile)

    def __setitem__(self, key, value):
        if not isinstance(key, str):
            raise TypeError("key should be of type str")
        self.append(self._transform(key), value)

    def __contains__(self, key):
        fp = self._file_path(self._transform(key))
        if fp.exists():
            return True
        else:
            return False

    def __len__(self):
        return len(list(self.path.glob(f"*.{self.suffix}")))

    def __iter__(self):
        for item in self.path.glob(f"*.{self.suffix}"):
            yield self._reverse_transform(str(item))

    def __delitem__(self, key):
        fp = self._file_path(self._transform(key))
        fp.unlink()

    def __getitem__(self, key):
        fp = self._file_path(self._transform(key))
        try:
            with self.file_io.open(fp) as infile:
                return json.load(infile)
        except FileNotFoundError:
            raise KeyError(key)
