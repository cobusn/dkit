#
# Copyright (C) 2016  Cobus Nel
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#
import unittest
import sys
sys.path.insert(0, "..")  # noqa

from tabulate import tabulate
from dkit.etl.reader import (
    BufferedFileReader,
    Bz2Reader,
    FileReader,
    GzipReader,
    Lz4Reader,
    LzmaReader,
    SharedMemoryReader,
)
from dkit.etl.source import (
    CsvDictSource,
    JsonSource,
    JsonlSource,
    PickleSource,
)
from dkit.etl.extensions.ext_msgpack import MsgpackSource
from dkit.etl.extensions.ext_xlsx import XLSXSource
from dkit.etl.extensions.ext_bxr import BXRSource
from test_a_sink_performance import ITERATIONS


class TestSourcePerformance(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.record = []

    def add_record(self, name, obj):
        record = {"name": name, "speed": obj.stats.seconds_elapsed,
                  "iterations": obj.stats.value}
        self.record.append(record)

    def test_jsonl_text(self):
        r = JsonlSource([FileReader("output/speed.jsonl")])
        self.assertEqual(len(list(r)), ITERATIONS)
        self.add_record("jsonl text", r)

    def test_json_text(self):
        r = JsonSource([FileReader("output/speed.json")])
        self.assertEqual(len(list(r)), ITERATIONS)
        self.add_record("json text", r)

    #
    # Fix This
    #
    def _test_jsonl_buffered_text(self):
        r = JsonlSource([BufferedFileReader("output/speed.jsonl")])
        self.assertEqual(len(list(r)), ITERATIONS)
        self.add_record("jsonl buffered text", r)

    def test_jsonl_bzip2(self):
        r = JsonlSource([Bz2Reader("output/speed.jsonl.bz2")])
        self.assertEqual(len(list(r)), ITERATIONS)
        self.add_record("jsonl bz2", r)

    def test_jsonl_lzma(self):
        r = JsonlSource([LzmaReader("output/speed.jsonl.xz")])
        self.assertEqual(len(list(r)), ITERATIONS)
        self.add_record("jsonl lzma", r)

    def test_jsonl_gzip(self):
        r = JsonlSource([GzipReader("output/speed.jsonl.gz")])
        self.assertEqual(len(list(r)), ITERATIONS)
        self.add_record("jsonl gzip", r)

    def test_csv_text(self):
        r = CsvDictSource([FileReader("output/speed.csv")])
        self.assertEqual(len(list(r)), ITERATIONS)
        self.add_record("csv text", r)

    def test_bxr_text(self):
        r = BXRSource([FileReader("output/speed.bxr")])
        self.assertEqual(len(list(r)), ITERATIONS)
        self.add_record("bxr text", r)

    def test_bxr_bz2(self):
        r = BXRSource([Bz2Reader("output/speed.bxr.bz2")])
        self.assertEqual(len(list(r)), ITERATIONS)
        self.add_record("bxr bz2", r)

    def test_bxr_lzma(self):
        r = BXRSource([LzmaReader("output/speed.bxr.xz")])
        self.assertEqual(len(list(r)), ITERATIONS)
        self.add_record("bxr lzma", r)

    def test_csv_bz2(self):
        r = CsvDictSource([Bz2Reader("output/speed.csv.bz2")])
        self.assertEqual(len(list(r)), ITERATIONS)
        self.add_record("csv bz2", r)

    def test_csv_lzma(self):
        r = CsvDictSource([LzmaReader("output/speed.csv.xz")])
        self.assertEqual(len(list(r)), ITERATIONS)
        self.add_record("csv bz2", r)

    def test_csv_gzip(self):
        r = CsvDictSource([GzipReader("output/speed.csv.gz")])
        self.assertEqual(len(list(r)), ITERATIONS)
        self.add_record("csv gz", r)

    def test_mpak(self):
        r = MsgpackSource([FileReader("output/speed.mpak", "rb")])
        self.assertEqual(len(list(r)), ITERATIONS)
        self.add_record("mpak", r)

    def test_mpak_bz2(self):
        r = MsgpackSource([Bz2Reader("output/speed.mpak.bz2", "rb")])
        self.assertEqual(len(list(r)), ITERATIONS)
        self.add_record("mpak bz2", r)

    def test_mpak_lz4(self):
        r = MsgpackSource([Lz4Reader("output/speed.mpak.lz4", "rb")])
        self.assertEqual(len(list(r)), ITERATIONS)
        self.add_record("mpak lz4", r)

    def test_pickle_shm(self):
        r = PickleSource([SharedMemoryReader("/speed.pkl")])
        self.assertEqual(len(list(r)), ITERATIONS)
        self.add_record("SHM Pickle", r)

    def test_pickle(self):
        r = PickleSource([FileReader("output/speed.pkl", "rb")])
        self.assertEqual(len(list(r)), ITERATIONS)
        self.add_record("Pickle", r)

    def test_pickle_bz2(self):
        r = PickleSource([Bz2Reader("output/speed.pkl.bz2", "rb")])
        self.assertEqual(len(list(r)), ITERATIONS)
        self.add_record("Pickle bz2", r)

    def test_pickle_lzma(self):
        r = PickleSource([LzmaReader("output/speed.pkl.xz", "rb")])
        self.assertEqual(len(list(r)), ITERATIONS)
        self.add_record("Pickle lzma", r)

    def test_mpak_lz4(self):
        r = PickleSource([Lz4Reader("output/speed.pkl.lz4", "rb")])
        self.assertEqual(len(list(r)), ITERATIONS)
        self.add_record("pickle lz4", r)

    def test_pickle_gzip(self):
        r = PickleSource([GzipReader("output/speed.pkl.gz", "rb")])
        self.assertEqual(len(list(r)), ITERATIONS)
        self.add_record("Pickle gzip", r)

    def _test_xslx_sink(self):
        r = XLSXSource(["output/speed.xlsx"])
        self.assertEqual(len(list(r)), ITERATIONS)
        self.add_record("xlsx", r)

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        lst = []
        for record in cls.record:
            iters = record["iterations"]
            name = record["name"]
            secs = record["speed"]
            rate = iters/float(secs)
            lst.append([iters, name, secs, rate])
        print("\n\n")
        lst = sorted(lst, key=lambda x: x[2])
        print(tabulate(lst, headers=["iters", "name", "sec's", "rate"],
                       tablefmt="psql", floatfmt=".2f"))


if __name__ == '__main__':
    unittest.main()
