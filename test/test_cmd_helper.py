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
sys.path.insert(0, "..")
import unittest
import argparse
from dkit.utilities.cmd_helper import StoreDict


class TestCmdHelper(unittest.TestCase):
    """Test the Introspect class"""

    def test_store_dict(self):
        """test parsing key value pairs"""
        args = ["--pairs", "val1=val_i,val2=val_ii"]
        test = {"val1": "val_i", "val2": "val_ii"}
        parser = argparse.ArgumentParser()
        parser.add_argument("--pairs", action=StoreDict, metavar="KEY1=VAL1,KEY2=VAL2...")
        parsed_args = parser.parse_args(args)
        self.assertEqual(parsed_args.pairs, test)


if __name__ == '__main__':
    unittest.main()
