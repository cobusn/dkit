# Copyright (c) 2019 Cobus Nel
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
danipulation routines.

=========== =============== =================================================
Jul 2019    Cobus Nel       Created
=========== =============== =================================================
"""
import unittest
import sys
sys.path.insert(0, "..")  # noqa
from dkit.etl import (source, sink, writer, reader)

codec_tests = {
    "pickle": {
        "source": source.PickleSource,
        "sink": sink.PickleSink,
    }
}

rw = {
    "SharedMemory": {
        "writer": writer.SharedMemoryWriter,
        "reader": reader.SharedMemoryReader,
        "file_name":  "/shared.writer.pkl",
        "reader_options": {"compresssion": None},
        "writer_options": {"compresssion": None},
    }
}


def load_data():
    with source.load("output/speed.jsonl") as input_data:
        return list(input_data)


class TestETL(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.data = load_data()

    def test_1_write_all_fields(self):
        for test in codec_tests:
            for _writer in rw:
                print(f"Testing {test} sink")
                sink_class = codec_tests[test]["sink"]
                writer_class = rw[_writer]["writer"]
                writer_opts = rw[_writer]["writer_options"]
                file_name = rw[_writer]["file_name"]
                with writer_class(file_name) as wfile:
                    snk = sink_class(wfile)
                    snk.process(self.data)

    def test_2_write_all_fields(self):
        for test in codec_tests:
            for _reader in rw:
                print(f"Testing {test} source")
                source_class = codec_tests[test]["source"]
                file_name = rw[_reader]["file_name"]
                reader_class = rw[_reader]["reader"]
                reader_opts = rw[_reader]["reader_options"]
                with reader_class(file_name) as rfile:
                    src = source_class([rfile])
                    data = list(src)
                self.assertEqual(len(data), len(self.data))


if __name__ == '__main__':
    unittest.main()
