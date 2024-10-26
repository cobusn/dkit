# Copyright (c) 2024 Cobus Nel
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
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
Simple Cache helper classes
"""
import pickle
from datetime import datetime
import dbm


class CacheEntry:

    def __init__(self, value):
        self.value = value
        self.timetamp = datetime.now()

    def serialize(self):
        return pickle.dumps(self)

    @classmethod
    def from_serialized(cls, obj):
        obj = pickle.loads(obj)
        return obj


class ObjectFileCache:

    def __init__(self, filename):
        self.filename = filename
        self.db = dbm.open(filename, "c")

    def __del__(self):
        if hasattr(self, "db") and self.db:
            self.db.close()

    def _make_key(self, key):
        key = pickle.dumps(key)
        return key

    def has_key(self, key):
        return self._make_key(key) in self.db

    def set_item(self, key, item):
        self.db[self._make_key(key)] = CacheEntry(item).serialize()

    def get_item(self, key):
        try:
            item = CacheEntry.from_serialized(self.db[self._make_key(key)])
        except KeyError:
            return None
        if not isinstance(item, CacheEntry):
            raise ValueError("Invalid content proceed with cuation")
        return item.value

    def delete_item(self, key):
        """remove item from database"""
        del self.db[self._make_key(key)]

    def items(self):
        """iterator for keys and values"""
        for k in self.db.keys():
            item = CacheEntry.from_serialized(self.db[k])
            yield pickle.loads(k), item.value

    def values(self):
        """iterator for database values"""
        for k in self.db.keys():
            item = CacheEntry.from_serialized(self.db[k])
            yield item.value

    def keys(self):
        """iterator of keys"""
        for k in self.db.keys():
            yield pickle.loads(k)
