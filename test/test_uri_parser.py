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
from dkit.exceptions import DKitParseException


class TestURIParser(unittest.TestCase):

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
            "parameters": None,
            "entity": None,
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
            # data["driver"] = "file"
            # data["compression"] = "gz"
            # data["encryption"] = "aes"
            # data["database"] = "input_files/sample.{}.gz.aes".format(dialect)
            # s = parse("input_files/sample.{}.gz.aes".format(dialect))
            # self.assertEqual(s, data)

    def test_sqlite_dialect(self):
        """file based sqlite dialect"""
        data = self.blank.copy()
        data["driver"] = "sql"
        data["dialect"] = "sqlite"
        data["database"] = "input_files/sample.db"
        s = parse("sqlite:///input_files/sample.db")
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

    def test_specified_pke_kdata(self):
        """file based data with specified dialect"""
        data = self.blank.copy()
        data["driver"] = "file"
        data["dialect"] = "pke"
        data["database"] = "input_files/sample.pke"
        s = parse("pke:///input_files/sample.pke")
        self.assertEqual(data, s)

    def test_hdf5_dialect(self):
        """hdf5 based file dialect"""
        data = self.blank.copy()
        data["driver"] = "hdf5"
        data["dialect"] = "hdf5"
        data["database"] = "input_files/sample.h5"
        s = parse("hdf5:///input_files/sample.h5")
        self.assertEqual(data, s)

    def test_t(self):
        uri = (
            "mysql+mysqldb://user:no&zzy@sample-db.co.za:99/database"
            "?sales=10"
            "&name=piet"
        )
        parse(uri)

    def test_network_db_endpoint(self):
        """sql based dialect"""
        tests = [
            [
                "mysql+mysqldb://user:now&zzy@sample-db.co.za:99/database",
                {
                    'username': 'user',
                    'password': 'now&zzy',
                    'host': 'sample-db.co.za',
                    'port': '99',
                    'database': 'database',
                    'dialect': "mysql+mysqldb",
                    'driver': "sql",
                    'parameters': {}
                }
            ],
            [
                (
                    "awsathena+rest://access_key:secret_key@athena."
                    "af-south-1.amazonaws.com:443/db"
                    "?s3_staging_dir=s3://test/results"
                ),
                {
                    'dialect': "awsathena+rest",
                    'username': 'access_key',
                    'password': 'secret_key',
                    'host': 'athena.af-south-1.amazonaws.com',
                    'port': '443',
                    'database': 'db',
                    'driver': "sql",
                    'parameters': {'s3_staging_dir': 's3://test/results'},
                }
            ],
        ]
        for test in tests:
            r = parse(test[0])
            self.assertEqual(r, test[1])

    def test_exception(self):
        with self.assertRaises(DKitParseException):
            parse("file.noname")
        with self.assertRaises(DKitParseException):
            parse("jso:///filename")


if __name__ == '__main__':
    unittest.main()
