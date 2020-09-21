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

import unittest
import sys
sys.path.insert(0, "..")  # noqa
from dkit.data.diff import Compare
from dkit.etl import source
from dkit import NA_VALUE
import random


N = 5  # Number of samples to modify


class TestDataDiff(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        with source.load("input_files/sample.jsonl") as infile:
            cls.a = list(infile)

    def sample_ids(self, rows, n):
        """sample random row numbers of population self.a"""
        pop = list(range(0, len(self.a)))
        return random.sample(pop, n)

    def sample_changed(self, n):
        a = [dict(x) for x in self.a[1:]]
        b = [dict(x) for x in self.a[:]]
        changes = self.sample_ids(a, n)
        for i in changes:
            b[i]["name"] = "changed"
        return self.a, b

    def sample_delta(self, n):
        changes = self.sample_ids(self.a, n)
        b = [dict(x) for x in self.a[:]]
        for i in changes:
            b[i]["score"] = b[i]["score"] - 10
        return self.a, b

    def sample_deleted(self, n):
        changes = self.sample_ids(self.a, n)
        b = [self.a[i] for i in range(len(self.a)) if i not in changes]
        return self.a, b

    def test_deleted(self):
        a, b = self.sample_deleted(N)
        c = Compare(a, b, keys=["id", "name"])
        self.assertEqual(len(list(c.deleted())), N)

    def test_deleted_nokeys(self):
        a, b = self.sample_deleted(N)
        c = Compare(a, b)
        self.assertEqual(len(list(c.deleted())), N)

    def test_deleted_huge(self):
        a, b = self.sample_deleted(N)
        c = Compare(self.a, b, keys=["id", "name"], huge=True)
        self.assertEqual(len(list(c.deleted())), N)

    def test_deleted_huge_nokeys(self):
        a, b = self.sample_deleted(N)
        c = Compare(self.a, b, huge=True)
        self.assertEqual(len(list(c.deleted())), N)

    def test_added(self):
        b, a = self.sample_deleted(N)
        c = Compare(a, b, keys=["id", "name"])
        self.assertEqual(len(list(c.added())), N)

    def test_added_nokey(self):
        b, a = self.sample_deleted(N)
        c = Compare(a, b)
        self.assertEqual(len(list(c.added())), N)

    def test_added_huge(self):
        b, a = self.sample_deleted(N)
        c = Compare(a, b, keys=["id", "name"], huge=True)
        self.assertEqual(len(list(c.added())), N)

    def test_added_huge_nokey(self):
        b, a = self.sample_deleted(N)
        c = Compare(a, b, huge=True)
        self.assertEqual(len(list(c.added())), N)

    def test_changed(self):
        a, b = self.sample_changed(N)
        c = Compare(a, b, keys=["id"])
        self.assertEqual(len(list(c.changed("name"))), N)
        new_rows = list(c.changed("name"))
        self.assertTrue("name.old" in new_rows[0])

    def test_changed_raise(self):
        """raise keyerror if no keys specified"""
        a, b = self.sample_changed(N)
        with self.assertRaises(KeyError) as _:
            c = Compare(a, b)
            _ = list(c.changed("name"))

    def test_deltas(self):
        a, b = self.sample_delta(N)
        c = Compare(a, b, keys=["id"])
        l_modified = list(c.deltas("score"))
        self.assertEqual(len(l_modified), N)
        for row in l_modified:
            self.assertEqual(row["score.delta"], -10)

    def test_deltas_na(self):
        a = [dict(i) for i in self.a[:]]
        b = [dict(i) for i in self.a[:]]
        b[0]["score"] = None

        c = Compare(a, b, keys=["id"])
        l_modified = list(c.deltas("score"))
        self.assertEqual(l_modified[0]["score.delta"], NA_VALUE)

    def test_deltas_huge(self):
        a, b = self.sample_delta(N)
        c = Compare(a, b, keys=["id"], huge=True)
        l_modified = list(c.deltas("score"))
        self.assertEqual(len(l_modified), N)
        for row in l_modified:
            self.assertEqual(row["score.delta"], -10)


if __name__ == '__main__':
    unittest.main()
