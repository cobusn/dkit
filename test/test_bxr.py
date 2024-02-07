# Copyright (c) 2017 Cobus Nel
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
import tabulate
import random
import unittest
from datetime import time, date
from dkit.data import fake_helper as fh
import sys
sys.path.insert(0, "..")
from dkit.data.bxr import (
    dump_iter,
    dump_mapping,
    dumps_iter,
    dumps_mapping,
    load,
    load_iter,
    loads_iter,
    loads
)


class TestBxr(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.records = []
        for i in fh.persons(10):
            i["birthday"] = i["birthday"].replace(microsecond=0)
            i["int"] = random.randint(-999999, 9999999)
            i["neg_int"] = -100
            i["float"] = -11.00001
            i["true"] = True
            i["false"] = False
            i["time"] = time(0, 10, 10)
            i["date"] = date.today()
            cls.records.append(i)
        cls.dict_obj = {}
        for i, row in enumerate(cls.records):
            cls.dict_obj[str(i)] = row

    def _print(self, l):
        print(tabulate.tabulate(l, headers="keys"))
        print("\n")

    def test_string_iterable(self):
        encoded = dumps_iter(self.records)
        decoded = list(loads_iter(encoded))
        for i in range(len(decoded)):
            for key in self.records[i]:
                self.assertEqual(
                    self.records[i][key],
                    decoded[i][key]
                )

    def test_file_iterable(self):
        with open("data/test.bxr", "w") as outfile:
            dump_iter(self.records, outfile)
        with open("data/test.bxr", "r") as infile:
            decoded = list(load_iter(infile))
        for i in range(len(decoded)):
            for key in self.records[i]:
                self.assertEqual(
                    self.records[i][key],
                    decoded[i][key]
                )

    def test_file(self):
        with open("data/test.bxr", "w") as outfile:
            dump_mapping(self.dict_obj, outfile)
        with open("data/test.bxr", "r") as infile:
            decoded = load(infile)
        for key1, value in self.dict_obj.items():
            row = decoded[key1]
            for key in value:
                self.assertEqual(value[key], row[key])

    def test_string(self):
        encoded = dumps_mapping(self.dict_obj)
        decoded = loads(encoded)
        for key1, value in self.dict_obj.items():
            row = decoded[key1]
            for key in value:
                self.assertEqual(value[key], row[key])


if __name__ == '__main__':
    unittest.main()
