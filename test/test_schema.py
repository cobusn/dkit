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
import sys; sys.path.insert(0, "..")
import unittest
from dkit.etl import (source, schema, reader, transform)


class TestSchema(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(TestSchema, cls).setUpClass()
        _schema_src = source.JsonlSource([reader.FileReader("input_files/sample.jsonl")])
        cls.validator = schema.EntityValidator.from_iterable(_schema_src)

    def test_coerce(self):
        src = source.JsonlSource([reader.FileReader("input_files/sample.jsonl")])
        coerced = transform.CoerceTransform(self.validator)(src)
        list(coerced)


if __name__ == '__main__':
    unittest.main()
