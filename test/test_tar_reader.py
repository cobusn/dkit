import unittest
import tarfile
import sys
sys.path.insert(0, "..")
from dkit.etl import reader, source


class TestTarReader(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        trfile = tarfile.open("input_files/input.tar.bz2", "w:bz2")
        trfile.add("input_files/sample.csv")
        trfile.add("input_files/sample.jsonl")
        trfile.close()

    def test_csv(self):
        obj = reader.TarFileReader("input_files/input.tar.bz2", r".*\.csv$")
        src = list(source.CsvDictSource(obj))
        self.assertEqual(len(src[0]), 7)
        self.assertEqual(len(src), 500)

    def test_json(self):
        obj = reader.TarFileReader("input_files/input.tar.bz2", r".*\.jsonl$")
        src = list(source.JsonlSource(obj))
        self.assertEqual(len(src[0]), 7)
        self.assertEqual(len(src), 500)


if __name__ == '__main__':
    unittest.main()
