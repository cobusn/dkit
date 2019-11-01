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

import sys
import unittest
import pickle
import yaml
sys.path.insert(0, "..")
from dkit.data.map_db import ObjectMapDB, FileObjectMapDB
from test_entity_map import TestClass


class TestEntityMapDB(unittest.TestCase):
    """Test the ObjectMap class"""

    @classmethod
    def setUpClass(cls):
        cls.db = ObjectMapDB(schema={"items": TestClass})
        cls.test_instance = TestClass(1, 2)

    def test_add_get(self):
        """
        assign and retrieve entity
        """
        self.db.items["one"] = self.test_instance
        instance = self.db.items["one"]
        self.assertEqual(instance, self.test_instance)

    def test_invalid_entity(self):
        """invalid attribute"""
        with self.assertRaises(KeyError) as _:
            i = self.db.invalid

    def test_dict(self):
        """as_dict and from_dict"""
        self.db.items["one"] = self.test_instance
        d = self.db.as_dict()
        d1 = self.db.from_dict(d).as_dict()
        self.assertEqual(d, d1)

    def test_serialize_json(self):
        """serialize to json"""
        json_db = FileObjectMapDB(schema={"items": TestClass})
        json_db.items["one"] = self.test_instance
        json_db.save("data/entity_map.json")
        json_db.load("data/entity_map.json")
        self.assertEqual(json_db.items["one"], self.test_instance)

    def test_serialize_yaml(self):
        """serialize to yaml"""
        json_db = FileObjectMapDB(schema={"items": TestClass}, codec=yaml)
        json_db.items["one"] = self.test_instance
        json_db.save("data/entity_map.yaml")
        json_db.load("data/entity_map.yaml")
        self.assertEqual(json_db.items["one"], self.test_instance)

    def test_serialize_pickle(self):
        """serialize to yaml"""
        json_db = FileObjectMapDB(schema={"items": TestClass}, codec=pickle, binary=True)
        json_db.items["one"] = self.test_instance
        json_db.save("data/entity_map.pickle")
        json_db.load("data/entity_map.pickle")
        self.assertEqual(json_db.items["one"], self.test_instance)

    def test_blank_file(self):
        """blank object if file does not exist"""
        json_db = FileObjectMapDB(schema={"items": TestClass})
        json_db.load("data/none.json")
        self.assertEqual(len(json_db.items), 0)


if __name__ == '__main__':
    unittest.main()
