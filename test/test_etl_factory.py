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

import unittest
import sys
sys.path.insert(0, "..") # noqa
from dkit.etl.utilities import source_factory, sink_factory
from dkit.parsers import uri_parser


class TestEtlFactories(unittest.TestCase):

    def test_sink_db_driver(self):
        uri_list = [
            "hdf5:///input_files/sample.h5#/data",
            "sqlite:///input_files/sample.db#data",
        ]
        for the_uri in uri_list:
            uri_struct = uri_parser.parse(the_uri)
            with sink_factory(uri_struct) as e:
                _ = e

    def test_sink_file_driver(self):
        uri_list = [
            "output/factory.jsonl",
            "ouput/factory.jsonl.xz",
            "ouput/factory.csv.bz2",
            "output/factory.xlsx",
            "output/factory.pkl",
            "output/factory.mpak",
        ]
        for the_uri in uri_list:
            uri_struct = uri_parser.parse(the_uri)
            with sink_factory(uri_struct) as e:
                _ = e

    def test_source_file_driver(self):
        uri_list = [
            "input_files/sample.jsonl",
            "input_files/sample.csv.bz2",
            "input_files/sample.jsonl.bz2",
            "input_files/excel_1.xlsx",
        ]
        for the_uri in uri_list:
            self.verify_list_from_uri(the_uri)

    def test_source_db_driver(self):
        uri_list = [
            "hdf5:///input_files/sample.h5#/data",
            "sqlite:///input_files/sample.db#data"
        ]
        for the_uri in uri_list:
            self.verify_list_from_uri(the_uri)

    def verify_list_from_uri(self, uri):
        uri_struct = uri_parser.parse(uri)
        with source_factory(uri_struct) as in_iter:
            inp = list(in_iter)
            self.assertGreater(len(inp), 1)


if __name__ == '__main__':
    unittest.main()
