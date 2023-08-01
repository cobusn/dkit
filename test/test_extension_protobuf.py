import sys; sys.path.insert(0, "..")  # noqa
import unittest
from zlib import adler32

from dkit.etl.extensions.ext_protobuf import SchemaGenerator
from dkit.etl.schema import EntityValidator


class TestPBSchemaExport(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.client = EntityValidator(
            {
                "name": {"type": "string"},
                "surname": {"type": "string"},
                "age": {"type": "integer"},
            }
        )

    def test_schema(self):
        g = SchemaGenerator(client=self.client)
        print(g.create_schema())
        h = adler32(g.create_schema().encode())
        print(h)
        self.assertTrue(h in (3140830570,))


if __name__ == '__main__':
    unittest.main()
