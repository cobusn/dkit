import unittest

from dkit.utilities.file_helper import sanitise_name


class TestSanitiseName(unittest.TestCase):
    def test_basic_normalization(self):
        self.assertEqual(sanitise_name("Hello World"), "hello_world")

    def test_unicode_normalization(self):
        self.assertEqual(sanitise_name("Café"), "cafe")
        self.assertEqual(sanitise_name("naïve façade"), "naive_facade")

    def test_reserved_names(self):
        self.assertEqual(sanitise_name("CON"), "con_")
        self.assertEqual(sanitise_name("nul"), "nul_")

    def test_max_length(self):
        name = "a" * 300
        self.assertEqual(len(sanitise_name(name)), 255)

    def test_empty_or_symbols(self):
        self.assertEqual(sanitise_name("!!!"), "file")
        self.assertEqual(sanitise_name("   "), "file")


if __name__ == "__main__":
    unittest.main()
