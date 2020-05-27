import os
import unittest
import sys
sys.path.insert(0, "..")  # noqa
from dkit.etl.source import CsvDictSource
from dkit.etl.sink import EncryptSink
from dkit.etl.reader import FileReader
from dkit.etl.source import EncryptSource
from dkit.etl.model import ModelManager
import gzip
import bz2
import lzma


class TestJsonlSink(unittest.TestCase):

    def setUp(self):
        self.data = list(CsvDictSource([FileReader(os.path.join("input_files", "sample.csv"))]))
        self.key = b'9LAL2zAP35zGxjOTQp8Y1jnjhsdo0ivOUyQ8p6HM0Wk='
        self.filename = "output/test.pke"

    def test_encrypt(self):
        snk = EncryptSink(self.filename, self.key)
        snk.process(self.data)

        # read
        src = EncryptSource(self.filename, self.key)
        data = list(src)
        self.assertEqual(self.data, data)

    def test_encrypt_gzip(self):
        snk = EncryptSink(f"{self.filename}.gz", self.key, gzip)
        snk.process(self.data)

        # read
        src = EncryptSource(f"{self.filename}.gz", self.key, gzip)
        data = list(src)
        self.assertEqual(self.data, data)

    def test_encrypt_bz2(self):
        snk = EncryptSink(f"{self.filename}.bz2", self.key, bz2)
        snk.process(self.data)

        # read
        src = EncryptSource(f"{self.filename}.bz2", self.key, bz2)
        data = list(src)
        self.assertEqual(self.data, data)

    def test_encrypt_lzma(self):
        snk = EncryptSink(f"{self.filename}.xz", self.key, lzma)
        snk.process(self.data)

        # read
        src = EncryptSource(f"{self.filename}.xz", self.key, lzma)
        data = list(src)
        self.assertEqual(self.data, data)

    def test_model(self):
        """
        test opening from model
        """
        m = ModelManager.from_file()

        with m.sink(f"{self.filename}.xz") as snk:
            snk.process(self.data)

        with m.source(f"{self.filename}.xz") as src:
            data = list(src)

        self.assertEqual(self.data, data)


if __name__ == '__main__':
    unittest.main()
