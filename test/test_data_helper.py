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

import sys
import unittest
import common
sys.path.insert(0, "..")
from dkit.data import helpers


class TestDataHelper(common.TestBase):
    """Test the data helper module"""

    def test_luhn_hash(self):
        self.assertEqual(
            helpers.luhn_hash(4992739871),
            6
        )

    def test_luhn_validate(self):
        numbers = [
            49927398716,
            12345678903,
            432039842402345,
            5253448,
        ]
        for n in numbers:
            self.assertEqual(
                helpers.validate_luhn_hash(n),
                True
            )


if __name__ == '__main__':
    unittest.main()
