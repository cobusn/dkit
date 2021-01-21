import sys
import unittest
sys.path.insert(0, "..")  # noqa
from dkit.data.iteration import chunker, glob_list


class TestIterHelpers(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.data = [
            {"name": "james", "surname": "bond", "score": 55,
             "address": {"prefix": 10, "city": "London", "country": "UK"}},
            {"name": "joan", "surname": "Jett", "score": 55,
             "address": {"prefix": 10, "city": "London", "country": "UK"}},
            {"name": "peter", "surname": "pan", "score": 45,
                "address": {"city": "New Yor", "country": "US"}},
            {"name": "atomic", "surname": "blonde", "score": 88,
                "address": {"city": "New York", "country": "US"}},
            {"name": "billy", "surname": "idol", "score": 32,
                "address": {"city": "New York", "country": "US"}},
        ]

    def test_chunker(self):
        """test chunker"""
        input_data = range(1000)
        for chunk in chunker(input_data, size=100):
            c = list(chunk)
            self.assertEqual(len(c), 100)

    def test_glob_list(self):
        """test glob_list"""
        c = glob_list(self.data, ["j*", "at*"], lambda x: x["name"])
        self.assertEqual(len(list(c)), 3)


if __name__ == "__main__":
    unittest.main()
