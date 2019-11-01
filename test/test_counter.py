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
'''
Created on 17 May 2011

@author: Cobus
'''

import sys
sys.path.insert(0, "..")
import unittest
import common
from dkit.utilities.instrumentation import Counter

class TestCounter(common.TestBase):
    """Test the Counter class"""

    def setUp( self ):
        super( TestCounter, self).setUp()
        self.t_obj = Counter()
        
    def test_100(self):
        """Test Couter() increment"""
        x=0
        while x < 100:
            self.t_obj.increment()
            x+=1
        self.assertEqual(self.t_obj.value, 100)
        
    def test_value_increment(self):
        """Test Counter() incrementing with specified value"""
        self.t_obj.increment(20)
        self.assertEqual(self.t_obj.value, 20)
        
    def test_assignment(self):
        """Test Counter() assignment"""
        a = Counter(10)
        b = Counter(10)
        c = a + b
        self.assertEqual(c.value, 20)
        
    def test_string_representation(self):
        """Test Counter() string representation"""
        a = Counter(10)
        self.assertEqual(str(a),'10')
        
    def tearDown( self ):
        super( TestCounter, self).tearDown()
        self.t_obj = None 
        
if __name__ == '__main__':
    unittest.main()