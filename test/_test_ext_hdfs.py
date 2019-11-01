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
import sys
import unittest
sys.path.insert(0, "..")
from dkit.etl.extensions.ext_hdfs import HDFSWriter, HDFSReader
from dkit.data.fake_helper import persons
from dkit.etl.sink import JsonlSink
from dkit.etl.source import JsonlSource


class TestHDFSExtension(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass

    def test_jsonl_sink(self):
        snk = JsonlSink(
            HDFSWriter("http://localhost:14001", "cobus", "data/persons.jsonl", overwrite=True)
        )
        snk.process(persons())

    def test_jsonl_source(self):
        src = JsonlSource(
            [HDFSReader("http://localhost:14001", "cobus", "data/persons.jsonl")]
        )
        data = list(src)
        self.assertEqual(len(data), 1000)


if __name__ == '__main__':
    unittest.main()
