# Copyright (c) 2020 Cobus Nel
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
Utilities to assist with identifying differences between two similar
datasets
"""
from dkit.utilities.file_helper import temp_filename
from dkit.data.helpers import md5_obj_hash
from dkit import messages
from .. import NA_VALUE
import shelve


class Compare(object):

    def __init__(self, a, b, keys=None, huge=False):
        self.a = a
        self.b = b
        self.keys = keys
        self.huge = huge
        self.__processed = False
        if huge:
            self.db_a = shelve.open(str(temp_filename()))
            self.db_b = shelve.open(str(temp_filename()))
        else:
            self.db_a = {}
            self.db_b = {}

    def __del__(self):
        if self.huge:
            self.db_a.clear()
            self.db_a.close()
            self.db_b.clear()
            self.db_b.close()

    def __build_indexes(self):
        """Build indexes.

        Index status is saved and will only be done once
        """
        if not self.__processed:
            self.db_a.update(self.__index(self.a))
            self.db_b.update(self.__index(self.b))
        self.__processed = True

    def __index(self, rows):
        keys = self.keys

        def keymap(row):
            """returns index as tuple of key values"""
            return tuple(row[k] for k in keys)

        def keymap_str(row):
            """returns index as md5 hash of bencode of key values"""
            return md5_obj_hash(tuple(row[k] for k in keys))

        if self.keys:
            # keys are specified
            if self.huge:
                # Assume database is a shelve that require string keys
                kf = keymap_str
            else:
                # database is a normal dict (can have any key)
                kf = keymap
        else:
            # no keys use hash of bencode of row
            kf = md5_obj_hash

        for row in rows:
            yield (kf(row), row)

    def added(self):
        """
        yields rows that are in set b but not set a
        """
        self.__build_indexes()
        for kb in self.db_b.keys():
            if kb not in self.db_a:
                yield self.db_b[kb]

    def deleted(self):
        """
        yields rows that are in set a but not set b
        """
        self.__build_indexes()
        for ka in self.db_a.keys():
            if ka not in self.db_b:
                yield self.db_a[ka]

    def __modified(self, fields):
        """yields modified rows"""
        db_a = self.db_a                    # optimize lookup
        if len(fields) > 0:
            for k, b in self.db_b.items():
                try:
                    a = db_a[k]
                except KeyError:
                    continue
                if any([a[field] != b[field] for field in fields]):
                    yield (k, a, b)
        else:
            for k, b in self.db_b.items():
                try:
                    a = db_a[k]
                except KeyError:
                    continue
                a_hash = md5_obj_hash(a)
                b_hash = md5_obj_hash(b)
                if a != b:
                    yield (k, a, b)

    def deltas(self, *fields):
        if not self.keys:  # pragma: no cover
            raise KeyError(messages.MSG_0025.format(self.__class__.__name__))
        self.__build_indexes()
        for k, a, b in self.__modified(fields):
            retval = dict(b)
            for field in fields:
                retval[f"{field}.old"] = a[field]
                try:
                    retval[f"{field}.delta"] = b[field] - a[field]
                except TypeError:
                    retval[f"{field}.delta"] = NA_VALUE
            yield retval

    def changed(self, *fields):
        """
        yields rows with modified values

        this method ignores new or missing rowsa

        params:
            - *fields: list of fields to verify

        yields:
            - rows in
        """
        if not self.keys:
            raise KeyError(messages.MSG_0025.format(self.__class__.__name__))
        self.__build_indexes()
        for k, a, b in self.__modified(fields):
            retval = dict(b)
            for field in fields:
                retval[f"{field}.old"] = a[field]
            yield retval
