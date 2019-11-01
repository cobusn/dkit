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
Utility modules for testing.
"""
import sys
sys.path.insert(0, "../dkit")

import configparser
import unittest
import logging

class TestBase(unittest.TestCase):
    """Base Class for testing"""

    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger("UnitTesting")
        handler = logging.FileHandler("unit_tests.log")
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s -  %(message)s")
        handler.setFormatter(formatter)
        cls.logger.addHandler(handler)
        cls.logger.setLevel(logging.DEBUG)   
        # cls.config = configparser.ConfigParser( )
        # cls.config.read("unit_tests.ini")

    @classmethod
    def tearDownClass(cls):
        super(TestBase, cls).tearDownClass()
        del cls.logger
        # del cls.config