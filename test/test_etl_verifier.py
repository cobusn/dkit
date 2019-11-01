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
import unittest
import random
import sys; sys.path.insert(0, "..")

from dkit.etl import verifier


class TestVerifier(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.v = verifier.ShelveVerifier("output/verifier.db", lambda x: x, flag="n")
        cls.alpha = [i for i in "abcdefghijklmnopqrstuvwxyz"]
        cls.num = [i for i in '1234567890']
        cls.alphanum = cls.alpha + cls.num
        random.shuffle(cls.alphanum)

    def test_a_mark_as_completed(self):
        """Mark alpha items as comleted"""
        lst = list(self.v.iter_mark_as_complete((i for i in self.alpha)))
        self.assertEqual(list(lst), self.alpha)

    def test_b_check_key(self):
        test = all([self.v.test_completed(i) for i in self.alpha])
        self.assertEqual(test, True)

    def test_c_iter_not_completed(self):
        lst = list(self.v.iter_not_completed(self.alphanum))
        self.assertEqual(all([i in self.num for i in lst]), True)
        self.assertEqual(len(lst), len(self.num))

    def test_d_verifiy_completed(self):
        lst = list(self.v.iter_mark_as_complete((i for i in self.alphanum)))
        self.assertEqual(all([i in self.num for i in lst]), True)
        self.assertEqual(len(lst), len(self.num))
        # Now all items should be completed
        self.assertEqual(all([self.v.test_completed(i) for i in self.alphanum]), True)


if __name__ == '__main__':
    unittest.main()
