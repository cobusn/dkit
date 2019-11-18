#
# Copyright (C) 2014  Cobus Nel
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

"""
test data manipulation routines.

=========== =============== =================================================
01 Dec 2016 Cobus Nel       Created
27 Jun 2019 Cobus Nel       Merged all manipulate tests
=========== =============== =================================================
"""

import datetime
import os
import random
import sys; sys.path.insert(0, "..")  # noqa
import unittest
from statistics import mean
import shelve
import common
from dkit.data.manipulate import (
    InferTypes,
    KeyIndexer,
    aggregate,
    aggregates,
    infer_type,
    iter_sample,
    merge,
    reduce_aggregate,
    melt
)
from dkit.etl.reader import FileReader
from dkit.etl.source import CsvDictSource
from dkit.data.containers import FlexShelve


class TestMelt(unittest.TestCase):

    def test_melt(self):
        data = [
            {
                "Year": "1920",
                "Jan": "40.6",
                "Feb": "40.8",
                "Mar": "44.4",
                "Apr": "46.7",
                "May": "54.1",
                "Jun": "58.5",
                "Jul": "57.7",
                "Aug": "56.4",
                "Sep": "54.3",
                "Oct": "50.5",
                "Nov": "42.9",
                "Dec": "39.8"
            }
        ]
        compare = [
            {'Year': '1920', 'Month': 'Jan', 'Temp': '40.6'},
            {'Year': '1920', 'Month': 'Feb', 'Temp': '40.8'},
            {'Year': '1920', 'Month': 'Mar', 'Temp': '44.4'},
            {'Year': '1920', 'Month': 'Apr', 'Temp': '46.7'},
            {'Year': '1920', 'Month': 'May', 'Temp': '54.1'},
            {'Year': '1920', 'Month': 'Jun', 'Temp': '58.5'},
            {'Year': '1920', 'Month': 'Jul', 'Temp': '57.7'},
            {'Year': '1920', 'Month': 'Aug', 'Temp': '56.4'},
            {'Year': '1920', 'Month': 'Sep', 'Temp': '54.3'},
            {'Year': '1920', 'Month': 'Oct', 'Temp': '50.5'},
            {'Year': '1920', 'Month': 'Nov', 'Temp': '42.9'},
            {'Year': '1920', 'Month': 'Dec', 'Temp': '39.8'},
        ]
        d = melt(data, id_fields=["Year"], var_name="Month", value_name="Temp")
        self.assertEqual(list(d), compare)


class TestAggregate(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.data = [
            {"region": "north", "product": "product 1", "amount": 1},
            {"region": "south", "product": "product 1", "amount": 1},
            {"region": "north", "product": "product 2", "amount": 1},
            {"region": "north", "product": "product 1", "amount": 1},
            {"region": "south", "product": "product 1", "amount": 1},
            {"region": "north", "product": "product 2", "amount": 1},
            {"region": "north", "product": "product 1", "amount": 1},
        ]

    def test_aggregates(self):
        """
        aggregates
        """
        agg = list(aggregates(
            self.data,
            ["region", "product"],
            [
                ("sum_region", "amount", sum),
                ("count_region", "amount", len),
            ]
        ))
        for item in agg:
            print(item)

    def test_aggregate(self):
        agg = aggregate(self.data, ["region", "product"], "amount")
        for row in agg:
            print(row)

    def test_reduce_aggregate(self):
        agg = reduce_aggregate(self.data, ["region", "product"], "amount")
        for row in agg:
            print(row)


class TestInferType(common.TestBase):
    """Test the Timer class"""

    def test_bool(self):
        """
        test infer bool types
        """
        tests = [True, False, "True", "False", "true", "no", "yes"]
        for test in tests:
            t = infer_type(test)
            self.assertEqual(t, bool)

    def test_int(self):
        """
        test infer int types
        """
        tests = [1, '12', '34', '-3', '3', ' 39 ']
        for test in tests:
            t = infer_type(test, strict=True)
            self.assertEqual(t, int)

        # using strict
        tests = [1, '12', '34', '-3', '3', ' 300 ', '300,0']
        for test in tests:
            t = infer_type(test, strict=False)
            self.assertEqual(t, int)

    def test_float(self):
        """
        test infer float types
        """
        tests = [1.0, '-0.00001', '12.1', '34.1', '-3.5', '3E5', ' 300.0 ', ' 3,00.0 ', '300,0.4E4']
        for test in tests:
            t = infer_type(test, strict=False)
            self.assertEqual(t, float)

    def test_str(self):
        """
        test infer str typs
        """
        tests = ["asdf", r"a@#%@", "a 2342", "A,2342", "1233ss"]
        for test in tests:
            t = infer_type(test, strict=False)
            self.assertEqual(t, str)

    def test_datetime(self):
        """
        test infer datetime types
        """
        tests = ["1 jan 2010", "5/5/2015", "5/5/05", "5-5-2015", "3 December 2016", "12:00"]
        for test in tests:
            t = infer_type(test, strict=False)
            self.assertEqual(t, datetime.datetime)


class TestInferTypes(common.TestBase):
    """Test the Timer class"""

    def test_1(self):
        data = [
            {"_str": "Str", "_int": "10", "_float": "10.2", "_datetime": "5 Jan 2016"},
            {"_str": "String", "_float": "100.2", "_datetime": "5 February 2017"},
        ]
        checker = InferTypes()
        types = checker(data)
        print(types)
        for row in checker.summary.values():
            print(row)


class TestIterSample(common.TestBase):
    """Test the Timer class"""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.the_iterator = list(range(1000))
        cls.sample_size = 100

    def test_n(self):
        """
        test sample_size
        """
        a = list(iter_sample(self.the_iterator, 1, self.sample_size))
        self.assertEqual(len(a), self.sample_size)

    def test_p(self):
        """
        test  probability
        """
        a = list(iter_sample(self.the_iterator, 0.3, self.sample_size))
        mean_diff = mean([j-i for i, j in zip(a[:-1], a[1:])])
        self.assertAlmostEqual(mean_diff/100, 0.03, 1)

    def test_n_infinite(self):
        """
        test infinite sample size
        """
        a = list(iter_sample(self.the_iterator, 0.8, 0))
        self.assertGreater(len(a), 0.7*len(self.the_iterator))

    def test_n_1(self):
        """
        test sample size of 1
        """
        a = list(iter_sample(self.the_iterator, 0.8, 1))
        self.assertEqual(len(a), 1)


class TestKeyIndexer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.backend = dict()

    def setUp(self):
        self.csv_source = CsvDictSource([FileReader(os.path.join("input_files", "sample.csv"))])
        self.year_index = KeyIndexer(self.csv_source, "year", backend=self.backend)
        self.year_index.process()

    def test_keys(self):
        self.assertGreater(len(self.year_index.keys()), 0)

    def test_contains_list(self):
        first_key = list(self.year_index)[0]
        first_item = self.year_index[first_key]
        self.assertEqual(isinstance(first_item, list), True)


class TestShelveKeyIndexer(TestKeyIndexer):

    @classmethod
    def setUpClass(cls):
        cls.backend = shelve.open("data/test_index.db")

    @classmethod
    def tearDownClass(cls):
        cls.backend.close()


class TestMerge(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.backend = None
        cls.create_data(cls)

    def setUp(self):
        super().setUp()

    def create_data(self):
        self.ld = []
        self.rd = []
        for i in range(10):
            self.ld.append({"key1": i, "key2": 2*i, "value": random.randint(0, 1000)})
            self.ld.append({"key1": i, "key2": 2*i, "value": random.randint(0, 1000)})
            self.rd.append({"keya": i, "keyb": 2*i, "value": random.randint(0, 1000)})
        self.ld.append({"key1": 88, "key2": 2.1, "value": random.randint(0, 1000)})
        self.rd.append({"keya": 99, "keyb": 2, "value": random.randint(0, 1000)})
        self.rd.append({"keya": 66, "keyb": 2, "value": random.randint(0, 1000)})

    def test_inner_join(self):
        m = list(merge(
            self.ld, self.rd,
            ["key1", "key2"], ["keya", "keyb"],
            backend=self.backend
        ))
        self.assertEqual(len(m), len(self.ld)-1)

    def test_inner_join2(self):
        """
        inner join with key specified as strings
        """
        m = list(merge(
            self.ld, self.rd,
            "key1", "keya",
            backend=self.backend
        ))
        self.assertEqual(len(m), len(self.ld)-1)

    def test_left_join(self):
        m = list(merge(self.ld, self.rd, ["key1", "key2"], ["keya", "keyb"], all_l=True))
        self.assertEqual(len(m), len(self.ld))

    def test_full_join(self):
        m = list(
            merge(
                self.ld, self.rd, ["key1", "key2"], ["keya", "keyb"],
                all_l=True, all_r=True
            )
        )
        self.assertEqual(len(m), len(self.ld) + 2)

    def tearDown(self):
        super().tearDown()
        self.t_obj = None


class TestShelveMerge(TestMerge):

    @classmethod
    def setUpClass(cls):
        cls.backend = FlexShelve("data/merge_shelve.db")
        cls.create_data(cls)

    @classmethod
    def tearDownClass(cls):
        cls.backend.close()


if __name__ == '__main__':
    unittest.main()
