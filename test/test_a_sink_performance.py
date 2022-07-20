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
import sys; sys.path.insert(0, "..") # noqa
import pickle
import _pickle
from tabulate import tabulate
from dkit.utilities.instrumentation import CounterLogger
from dkit.etl.writer import (FileWriter, Bz2Writer, LzmaWriter, GzipWriter,
                             Lz4Writer, SharedMemoryWriter)
from dkit.etl.sink import JsonlSink, CsvDictSink, PickleSink, JsonSink
from dkit.etl.extensions.ext_xlsx import XlsxSink
from dkit.etl.extensions.ext_msgpack import MsgpackSink
from dkit.etl.extensions.ext_bxr import BXRSink
from dkit.etl.utilities import Dumper
from dkit.etl.extensions.ext_avro import AvroSink
from datetime import datetime


ITERATIONS = 10_000


class TestSinkPerformance(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.iterations = ITERATIONS
        super().setUpClass()
        cls.record = []

    def setUp(self):
        self.gen = ({"id": i, "int": 1, "str": "aaa", "str2": "bbb", "str3": "ccc",
                    "time": datetime.now(), "float": 11.5} for i in range(self.iterations))

    def add_record(self, name, obj):
        record = {"name": name, "speed": obj.seconds_elapsed,
                  "iterations": obj.value}
        self.record.append(record)

    def test_dump_pickle(self):
        c = CounterLogger().start()
        ll = Dumper("output/speed.dump", pickler=pickle).dump(list(self.gen))
        c.increment(len(ll))
        c.stop()
        self.add_record("pickle dumper", c)

    def test_dump_cpickle(self):
        c = CounterLogger().start()
        ll = Dumper("output/speed.dump", pickler=_pickle).dump(list(self.gen))
        c.increment(len(ll))
        c.stop()
        self.add_record("_pickle dumper", c)

#     def test_dump_marshal(self):
#         c =  CounterLogger().start()
#         l = dumper("output/speed.dump", pickler=marshal).dump(list(self.gen))
#         c.increment(len(l))
#         c.stop()
#         self.add_record("marshal dumper", c)

    def _test_bsonl_text(self):
        """add in future"""
        # obj = BsonlSink(FileWriter("output/speed.bsonl", mode="wb"))
        # obj.process(self.gen)
        # self.add_record("bsonl text", obj.stats)
        pass

    def test_avro_deflate(self):
        obj = AvroSink(FileWriter("output/speed_deflate.avro", "wb"), codec="deflate")
        obj.process(self.gen)
        self.add_record("avro deflate", obj.stats)

    def test_avro_null(self):
        obj = AvroSink(FileWriter("output/speed_null.avro", "wb"), codec="null")
        obj.process(self.gen)
        self.add_record("avro no compression", obj.stats)

    def test_avro_snappy(self):
        obj = AvroSink(FileWriter("output/speed_snappy.avro", "wb"))
        obj.process(self.gen)
        self.add_record("avro snappy", obj.stats)

    def test_json_text(self):
        if self.iterations <= 10_000:
            obj = JsonSink(FileWriter("output/speed.json"))
            obj.process(self.gen)
            self.add_record("json text", obj.stats)
        else:
            print("skipping json test")

    def test_jsonl_text(self):
        if self.iterations <= 10_000:
            obj = JsonlSink(FileWriter("output/speed.jsonl"))
            obj.process(self.gen)
            self.add_record("jsonl text", obj.stats)
        else:
            print("skipping jsonl test")

    def test_jsonl_bzip2(self):
        if self.iterations <= 10_000:
            obj = JsonlSink(Bz2Writer("output/speed.jsonl.bz2"))
            self.add_record("jsonl bz2", obj.process(self.gen).stats)
        else:
            print("skipping json bzip tests")

    def test_jsonl_lzma(self):
        if self.iterations <= 10_000:
            obj = JsonlSink(LzmaWriter("output/speed.jsonl.xz"))
            self.add_record("jsonl lzma", obj.process(self.gen).stats)
        else:
            print("skipping json lzma tests")

    def test_jsonl_gzip(self):
        if self.iterations <= 10_000:
            obj = JsonlSink(GzipWriter("output/speed.jsonl.gz"))
            self.add_record("jsonl gzip", obj.process(self.gen).stats)
        else:
            print("skipping json gzip tests")

    def test_bxr(self):
        if self.iterations <= 10_000:
            obj = BXRSink(FileWriter("output/speed.bxr"))
            self.add_record("bxr sink", obj.process(self.gen).stats)
        else:
            print("skipping BXR tests")

    def test_bxr_bz2(self):
        if self.iterations <= 10_000:
            obj = BXRSink(Bz2Writer("output/speed.bxr.bz2"))
            self.add_record("bxr bz2 sink", obj.process(self.gen).stats)
        else:
            print("skipping BXR tests")

    def test_bxr_lzma(self):
        if self.iterations <= 10_000:
            obj = BXRSink(LzmaWriter("output/speed.bxr.xz"))
            self.add_record("bxr lzma sink", obj.process(self.gen).stats)
        else:
            print("skipping BXR lzma tests")

    def test_pickle(self):
        obj = PickleSink(FileWriter("output/speed.pkl", "wb"))
        self.add_record("pickle sink", obj.process(self.gen).stats)

    def test_pickle_shm(self):
        """shared memory"""
        obj = PickleSink(SharedMemoryWriter("speed.pkl"))
        self.add_record("pickle shm sink", obj.process(self.gen).stats)

    def test_pickle_bz2(self):
        obj = PickleSink(Bz2Writer("output/speed.pkl.bz2", "wb"))
        self.add_record("pickle bz2 sink", obj.process(self.gen).stats)

    def test_pickle_lzma(self):
        obj = PickleSink(LzmaWriter("output/speed.pkl.xz", "wb"))
        self.add_record("pickle lzma sink", obj.process(self.gen).stats)

    def test_pickle_gzip(self):
        obj = PickleSink(GzipWriter("output/speed.pkl.gz", "wb"))
        self.add_record("pickle gzip sink", obj.process(self.gen).stats)

    def test_pickle_lz4(self):
        obj = PickleSink(Lz4Writer("output/speed.pkl.lz4", "wb"))
        self.add_record("pickle lz4", obj.process(self.gen).stats)

    def test_csv_text(self):
        obj = CsvDictSink(FileWriter("output/speed.csv"))
        self.add_record("csv text", obj.process(self.gen).stats)

    def test_csv_bz2(self):
        obj = CsvDictSink(Bz2Writer("output/speed.csv.bz2"))
        self.add_record("csv bz2", obj.process(self.gen).stats)

    def test_csv_lzma(self):
        obj = CsvDictSink(LzmaWriter("output/speed.csv.xz"))
        self.add_record("csv lzma", obj.process(self.gen).stats)

    def test_csv_gzip(self):
        obj = CsvDictSink(GzipWriter("output/speed.csv.gz"))
        self.add_record("csv gzip", obj.process(self.gen).stats)

    def __test_msgpack_raw(self):
        obj = MsgpackSink(FileWriter("output/speed.mpak", "wb"))
        self.add_record("msgpack raw", obj.process(self.gen).stats)

    def test_msgpack_text(self):
        obj = MsgpackSink(FileWriter("output/speed.mpak", "wb"))
        self.add_record("msgpack datetime", obj.process(self.gen).stats)

    def test_msgpack_bz2(self):
        obj = MsgpackSink(Bz2Writer("output/speed.mpak.bz2", "wb"))
        self.add_record("msgpack bz2", obj.process(self.gen).stats)

    def test_msgpack_lz4(self):
        obj = MsgpackSink(Lz4Writer("output/speed.mpak.lz4", "wb"))
        self.add_record("msgpack lz4", obj.process(self.gen).stats)

    def _test_xslx_source(self):
        obj = XlsxSink("output/speed.xlsx")
        self.add_record("xlsx", obj.process(self.gen).stats)

    @classmethod
    def tearDownClass(cls):
        super(TestSinkPerformance, cls).tearDownClass()
        lst = []
        for record in cls.record:
            iters = record["iterations"]
            name = record["name"]
            secs = record["speed"]
            rate = iters/float(secs)
            lst.append([iters, name, secs, rate])
        s_lst = sorted(lst, key=lambda x: x[2])

        print("\n\n")
        print(tabulate(s_lst, headers=["iters", "name", "sec's", "rate"],
                       tablefmt="psql", floatfmt=".2f"))


if __name__ == '__main__':
    unittest.main()
