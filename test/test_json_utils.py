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

import math
import sys; sys.path.insert(0, "..")  # noqa
import unittest
from datetime import datetime, date

from dkit.data import json_utils as ju


sys.path.insert(0, "..")  # noqa

class TestBytesEncoder(unittest.TestCase):

    def setUp(self):
        self.serializer = ju.JsonSerializer(
            ju.BytesCodec()
        )

    def test_encoder_bytes(self):
        obj = {"bytes value": b'1234'}
        encoded = self.serializer.dumps(obj)
        decoded = self.serializer.loads(encoded)
        self.assertEqual(obj, decoded)


class TestJsonUtilsDateEncoder(unittest.TestCase):

    def setUp(self):
        self.serializer = ju.JsonSerializer(
            ju.DateTimeCodec(),
            ju.DateCodec(),
        )

    def test_encoder_datetime(self):
        obj = {'language': 'python', 'datetime': datetime.now()}
        encoded = self.serializer.dumps(obj)
        decoded = self.serializer.loads(encoded)
        self.assertTrue(isinstance(decoded["datetime"], datetime))
        self.assertEqual(obj['datetime'], decoded['datetime'])

    def test_encoder_date(self):
        obj = {'language': 'python', 'date': date.today()}
        encoded = self.serializer.dumps(obj)
        decoded = self.serializer.loads(encoded)
        self.assertTrue(isinstance(decoded["date"], date))
        self.assertEqual(obj['date'], decoded['date'])

    def _test_encoder_nan(self):
        #
        # Disabled this test. Nan values are handled as None in the library
        #
        obj = {"value": math.nan}
        encoded = self.serializer.dumps(obj)
        print(encoded)
        decoded = self.serializer.loads(encoded)
        self.assertTrue(isinstance(decoded["value"], float))
        self.assertEqual(obj['value'], decoded['value'])


class TestJsonUtilsDateDictEncoder(TestJsonUtilsDateEncoder):

    def setUp(self):
        self.serializer = ju.JsonSerializer(
            ju.DateTimeDictCodec(),
            ju.DateDictCodec(),
        )


if __name__ == '__main__':
    unittest.main()
