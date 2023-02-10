import sys
import unittest
sys.path.insert(0, "..")  # noqa
from dkit.data.iteration import (
    chunker, glob_list, first_n, last_n, pairwise, long_range
)


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

    def test_first_n(self):
        l5 = first_n(range(100), 5)
        self.assertEqual(
            [0, 1, 2, 3, 4],
            list(l5)
        )

        # Invalid input
        l5 = first_n(range(100), -1)
        self.assertEqual(
            [],
            list(l5)
        )

        # Empty input
        l5 = first_n((), -1)
        self.assertEqual(
            [],
            list(l5)
        )

    def test_last_n(self):

        # Basic functionality
        l5 = last_n(range(100), 5)
        self.assertEqual(
            [95, 96, 97, 98, 99],
            list(l5)
        )

        # Invalid input
        l5 = last_n(range(100), -1)
        self.assertEqual(
            [],
            list(l5)
        )

        # Empty input
        l5 = last_n((), -1)
        self.assertEqual(
            [],
            list(l5)
        )

    def test_glob_list(self):
        """test glob_list"""
        c = glob_list(self.data, ["j*", "at*"], lambda x: x["name"])
        self.assertEqual(len(list(c)), 3)

    def test_pairwise(self):
        a = list(pairwise("abcd"))
        self.assertEqual(
            a,
            [('a', 'b'), ('b', 'c'), ('c', 'd')]

        )

    def test_long_range(self):
        ans = list(long_range(0, 5, 1))
        self.assertEqual(
            ans,
            [0, 1, 2, 3, 4, 5]
        )

        ans = list(long_range(0, 50, 10))
        self.assertEqual(
            ans,
            [0, 10, 20, 30, 40, 50]
        )


if __name__ == "__main__":
    unittest.main()
