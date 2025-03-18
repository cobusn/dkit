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
transaction
"""
from collections.abc import Mapping
from pathlib import Path
import json


class JSONDB(Mapping):
    """json file based db

    args:
        - path: file path to folder used to store json files
        - suffix: file suffix for json files (e.g. json)
    """
    def __init__(self, path: str, suffix="json"):
        self.path: Path = Path(path)
        self.suffix = suffix

    def _file_path(self, key):
        return self.path / f"{self.transform(key)}.{self.suffix}"

    def _reverse_transform(self, filename: str):
        """translate filname back to key"""
        key = filename.removesuffix(f".{self.suffix}")
        return key.removeprefix(f"{self.path}/")

    def _transform(self, key):
        """overide this if your keys have special characters
        that cannot be used in filenames
        """
        return key

    def append(self, key, value):
        fname = self._file_path(key)
        with open(fname, "wt") as outfile:
            json.dump(value, outfile)

    def __setitem__(self, key, value):
        self.append(key, value)

    def __contains__(self, key):
        fp = self._file_path(key)
        if fp.exists():
            return True
        else:
            return False

    def __len__(self):
        return len(list(self.path.glob(f"*.{self.suffix}")))

    def __iter__(self):
        for item in self.path.glob(f"*.{self.suffix}"):
            yield self._reverse_transform(str(item))

    def __getitem__(self, key):
        fp = self._file_path(key)
        with open(fp) as infile:
            return json.load(infile)
