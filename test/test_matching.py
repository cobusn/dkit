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

"""
Test cases for data matching routines
=========== =========== =================================================
22 May 2017 Cobus Nel   Initial Version
23 May 2017 Cobus Nel   Added tests for supporting functions
13 Jun 2018 Cobus Nel   Maintenance
=========== =========== =================================================
"""

import copy
import random
import unittest

from dkit.utilities import log_helper as clog
from faker import Factory

import sys; sys.path.insert(0, "..")
from dkit.data import matching as mg


ROWS = 500
SEED_LENGTH = 10
fake = Factory.create()


def left_data():
    for i in range(ROWS):
        yield {
            "id": fake.uuid4(),
            "name": fake.name(),
            "job": fake.job(),
        }


def right_data(seed):
    right_new = [
        {
            "id": fake.uuid4(),
            "name": fake.name(),
            "job": fake.job()
        }
        for i in range(ROWS - len(seed))
    ]
    right_new = seed + right_new
    random.shuffle(right_new)
    for row in right_new:
        row["idr"] = fake.uuid4()
        del row["id"]
    return right_new


class TestMatching(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.logger = clog.stdout_logger()
        cls.logger.info("Creating left hand data")
        l_data = list(left_data())
        cls.logger.info("Creating right hand data")
        r_data = right_data(copy.deepcopy(l_data[0:SEED_LENGTH]))
        random.shuffle(l_data)
        cls.left_data = l_data
        cls.right_data = r_data

    # def setUp(self):
        cls.f1 = mg.FieldSpec("name", "name", 2)
        cls.f2 = mg.FieldSpec("job", "job", 1, stop_words=['pty', 'ltd'])
        cls.m = mg.DictMatcher(
            cls.left_data,
            cls.right_data,
            "id",
            "idr",
            [cls.f1, cls.f2],
            logger=cls.logger,
            count_trigger=100
        )

    def test_match_iterator(self):
        """test using iterator for extracting matches"""
        matches = list(self.m)
        self.assertGreaterEqual(len(matches), SEED_LENGTH)

    def test_matches_property(self):
        """test using matches property for accessing matches"""
        matches = self.m.matches
        self.saved_matches = self.m
        self.assertGreaterEqual(len(matches), SEED_LENGTH)

    def test_inner_join(self):
        """test joining matched data"""
        join = mg.inner_join([self.m], self.left_data, self.right_data)
        join = list(join)
        self.assertGreaterEqual(len(join), SEED_LENGTH)

    def test_unmatched_left(self):
        """test extracting unmatched left hand data"""
        unmatched = list(mg.unmatched_left([self.m], self.left_data))
        self.assertLessEqual(len(unmatched), ROWS-SEED_LENGTH)

    def test_unmatched_right(self):
        """test extracting unmatched right hand data"""
        unmatched = list(mg.unmatched_right([self.m], self.right_data))
        self.assertLessEqual(len(unmatched), ROWS-SEED_LENGTH)


if __name__ == '__main__':
    unittest.main()
