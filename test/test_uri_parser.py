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
sys.path.insert(0, "..")  # noqa
from dkit.etl import source
from dkit.parsers.uri_parser import parse
from dkit.parsers import uri_parser
from dkit.exceptions import CkitParseException


class TestEndpointFactory(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.source_tests = {
            "filename.csv": source.CsvDictSource,
            "filename.json": source.JsonlSource,
        }
        cls.blank = {
            "driver": None,
            "dialect": None,
            "compression": None,
            "database": None,
            "username": None,
            "password": None,
            "host": None,
            "port": None,
            "entity": None,
            "encryption": None,
            "filter": None
        }

    def test_file_dialects(self):
        """file based dialect"""
        for dialect in uri_parser.FILE_DIALECTS:
            data = self.blank.copy()
            data["dialect"] = dialect

            # uncompressed
            data["driver"] = "file"
            data["database"] = "input_files/sample.{}".format(dialect)
            s = parse("input_files/sample.{}".format(dialect))
            self.assertEqual(s, data)

            # compressed
            data["driver"] = "file"
            data["compression"] = "gz"
            data["database"] = "input_files/sample.{}.gz".format(dialect)
            s = parse("input_files/sample.{}.gz".format(dialect))
            self.assertEqual(s, data)

            # encrypted
            data["driver"] = "file"
            data["compression"] = "gz"
            data["encryption"] = "aes"
            data["database"] = "input_files/sample.{}.gz.aes".format(dialect)
            s = parse("input_files/sample.{}.gz.aes".format(dialect))
            self.assertEqual(s, data)

    def test_sqlite_dialect(self):
        """file based sqlite dialect"""
        data = self.blank.copy()
        data["driver"] = "sqlite"
        data["entity"] = "sales"
        data["dialect"] = "sqlite"
        data["database"] = "input_files/sample.db"
        s = parse("sqlite:///input_files/sample.db?sales")
        self.assertEqual(data, s)

    def test_stdout_data(self):
        """file based data with specified dialect"""
        data = self.blank.copy()
        data["driver"] = "file"
        data["dialect"] = "jsonl"
        data["database"] = "stdio"
        s = parse("jsonl:///stdio")
        self.assertEqual(data, s)

    def test_specified_file_data(self):
        """file based data with specified dialect"""
        data = self.blank.copy()
        data["driver"] = "file"
        data["dialect"] = "jsonl"
        data["database"] = "input_files/sample.db"
        s = parse("jsonl:///input_files/sample.db")
        self.assertEqual(data, s)

    def test_specified_json_data(self):
        """file based data with specified dialect"""
        data = self.blank.copy()
        data["driver"] = "file"
        data["dialect"] = "json"
        data["database"] = "input_files/sample.json"
        s = parse("json:///input_files/sample.json")
        self.assertEqual(data, s)

    def test_specified_mpak_data(self):
        """file based data with specified dialect"""
        data = self.blank.copy()
        data["driver"] = "file"
        data["dialect"] = "mpak"
        data["database"] = "input_files/sample.mpak"
        s = parse("mpak:///input_files/sample.mpak")
        self.assertEqual(data, s)

    def test_hdf5_dialect(self):
        """hdf5 based file dialect"""
        data = self.blank.copy()
        data["driver"] = "hdf5"
        data["entity"] = "/sales/jan"
        data["dialect"] = "hdf5"
        data["database"] = "input_files/sample.h5"
        data["filter"] = "name=='piet'"
        s = parse("hdf5:///input_files/sample.h5?/sales/jan#[name=='piet']")
        self.assertEqual(data, s)

    def test_network_db_endpoint_function(self):
        """sql based dialect"""
        tests = [
            [
                "mysql://user:now#zzy@sample-db.co.za:99/database?sales#[a=10]",
                {'username': 'user', 'password': 'now#zzy', 'host': 'sample-db.co.za',
                 'port': '99', 'database': 'database', 'entity': 'sales',
                 'dialect': "mysql", 'filter': 'a=10', 'driver': "mysql+mysqldb",
                 'compression': None, 'encryption': None}
            ],
        ]
        for test in tests:
            r = parse(test[0])
            self.assertEqual(r, test[1])

    def test_exception(self):
        with self.assertRaises(CkitParseException):
            parse("file.noname")
        with self.assertRaises(CkitParseException):
            parse("jso:///filename")


if __name__ == '__main__':
    unittest.main()
