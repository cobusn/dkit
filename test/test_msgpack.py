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
import datetime
import sys; sys.path.insert(0, "..")
import decimal
from dkit.data import msgpack_utils


class TestMsgpack(unittest.TestCase):

    def setUp(self):
        self.enc = msgpack_utils.MsgpackEncoder()

    def test_encode_datetime(self):
        now = datetime.datetime.now()
        encoded = self.enc.pack(now)
        self.assertEqual(now, self.enc.unpack(encoded))

    def test_encode_date(self):
        now = datetime.date.today()
        encoded = self.enc.pack(now)
        self.assertEqual(now, self.enc.unpack(encoded))

    def test_encode_decimal(self):
        d = decimal.Decimal(10)
        encoded = self.enc.pack(d)
        self.assertEqual(
            d,
            self.enc.unpack(encoded)
        )

    def test_listof_items(self):
        data = ["str", 2.2, datetime.datetime.now(), 1, b"11"]
        encoded = self.enc.pack(data)
        # lists are transformed as tuples
        self.assertEqual(tuple(data), self.enc.unpack(encoded))


if __name__ == '__main__':
    unittest.main()
