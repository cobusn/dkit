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

import sys; sys.path.insert(0, "..")
import unittest

from dkit.data.manipulate import InferTypes
import common

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
        #print()
        #for key, value in types.items():
        #    print(key, value)

if __name__ == '__main__':
    unittest.main()