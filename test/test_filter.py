import random
import sys
import timeit
import unittest
import json
from re import RegexFlag

sys.path.insert(0, "..")  # noqa
from dkit.data.filters import Proxy, ExpressionFilter, search_filter, match_filter
from dkit.etl.utilities import source_factory
from dkit.parsers import uri_parser

expr = Proxy()

#
# data courtesy from https://github.com/benoitvallon/100-best-books
#


class TestRegexFilter(unittest.TestCase):
    """
    Test Regular expression filter
    """
    @classmethod
    def setUpClass(cls):
        with open("input_files/books.json") as infile:
            cls.data = list(json.load(infile))

    def test_grep_all(self):
        results = list(filter(search_filter("United.*"), self.data))
        self.assertEqual(
            len(results),
            21
        )

    def test_search_field(self):
        is_english = search_filter(r"Eng", ["language"])
        result = [i for i in self.data if is_english(i)]
        self.assertEqual(
            len(result),
            30
        )

    def test_match_field(self):
        is_english = match_filter(r"Eng", ["language"])
        result = [i for i in self.data if is_english(i)]
        self.assertEqual(
            len(result),
            29
        )

    def test_flags(self):
        is_english = match_filter(r"english", ["language"], RegexFlag.IGNORECASE)
        result = [i for i in self.data if is_english(i)]
        self.assertEqual(
            len(result),
            29
        )


class TestExpressionFilter(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        with source_factory(uri_parser.parse("input_files/sample.jsonl.bz2")) as indata:
            cls.src = list(indata)

    def test_filter(self):
        the_len = len(list(filter(lambda x: x["score"] > 50, self.src)))
        i_filter = ExpressionFilter("${score} > 50")
        ls = list(filter(i_filter, self.src))
        self.assertEqual(len(ls), the_len)


class TestFilter(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.data = [
            {"name": "james", "surname": "bond", "score": 55,
             "address": {"prefix": 10, "city": "London", "country": "UK"}},
            {"name": "peter", "surname": "pan", "score": 45,
                "address": {"city": "New Yor", "country": "US"}},
            {"name": "atomic", "surname": "blonde", "score": 88,
                "address": {"city": "New York", "country": "US"}},
            {"name": "billy", "surname": "idol", "score": 32,
                "address": {"city": "New York", "country": "US"}},

        ]

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_x_penalty(self):
        """test performance penalty"""
        def data():
            return ({"data": random.random()} for i in range(10000))

        def test_lambda():
            return len(list(filter(lambda x: x["data"] >= 0.5, data())))

        test = expr.data >= 0.5

        def test_proxy():
            return len(list(filter(test, data())))

        i = timeit.timeit(test_lambda, number=100)
        p = timeit.timeit(test_proxy, number=100)
        print("\nPenalty using proxy class instead of lambda is: {:.2f}".format(p/i))

    def test_exists(self):
        """Test existance of key"""
        test = expr.address.prefix.exists()
        result = [i for i in self.data if test(i)]
        self.assertEqual(len(result), 1)

    def test_exists_invert(self):
        test = ~ expr.address.prefix.exists()
        result = [i for i in self.data if test(i)]
        self.assertEqual(len(result), 3)

    def test_exists_and(self):
        test = (expr.address.prefix.exists()) & (expr.name == 'james')
        self.assertEqual(len([i for i in self.data if test(i)]), 1)

    def test_invert(self):
        """Test invert operator ~"""
        test = ~ (expr.address.city == 'London')
        result = [i for i in self.data if test(i)]
        self.assertEqual(len(result), 3)

    def test_invert_and(self):
        """Test invert with and"""
        test = (~ (expr.address.city == 'London')) & (expr.name == "peter")
        result = [i for i in self.data if test(i)]
        self.assertEqual(len(result), 1)

    def test_and(self):
        """Test and operator"""
        test = ((expr.name == 'james') & (expr.score <= 55))
        result = [i for i in self.data if test(i)]
        self.assertEqual(len(result), 1)

    def test_or(self):
        """Test or operator"""
        test = ((expr.name == 'james') | (expr.name == 'peter'))
        result = [i for i in self.data if test(i)]
        self.assertEqual(len(result), 2)

    def test_eq_numeric(self):
        """Test application of equal operator"""
        test = expr.score == 55
        result = [i for i in self.data if test(i)]
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["name"], "james")

    def test_eq_string(self):
        """Test applicaton of equal operator for strings"""
        test = expr.name == "james"
        result = [i for i in self.data if test(i)]
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["name"], "james")

    def test_ne_numeric(self):
        """Test application of not equal operator"""
        test = expr.score != 45
        result = [i for i in self.data if test(i)]
        self.assertEqual(len(result), 3)

    def test_lt(self):
        """Test application of less than"""
        test = expr.score < 55
        result = [i for i in self.data if test(i)]
        self.assertEqual(len(result), 2)

    def test_le(self):
        """Test application of less than or equal to"""
        test = expr.score <= 55
        result = [i for i in self.data if test(i)]
        self.assertEqual(len(result), 3)

    def test_le_invert(self):
        test = ~ (expr.score <= 55)
        result = [i for i in self.data if test(i)]
        self.assertEqual(len(result), 1)

    def test_gt(self):
        """Test application of more than"""
        test = expr.score > 55
        result = [i for i in self.data if test(i)]
        self.assertEqual(len(result), 1)

    def test_ge(self):
        """Test application of more than or equal to"""
        test = expr.score >= 55
        result = [i for i in self.data if test(i)]
        self.assertEqual(len(result), 2)

    def test_isin(self):
        """Test application of isin"""
        test = expr.name.isin("james", "Jane", "Tarzan")
        result = [i for i in self.data if test(i)]
        self.assertEqual(len(result), 1)

    def test_isin_invert(self):
        test = ~ expr.name.isin("james", "Jane", "Tarzan")
        result = [i for i in self.data if test(i)]
        self.assertEqual(len(result), 3)

    def test_match(self):
        """Test regex matching"""
        test = expr.name.match(r"^ja.*")
        result = [i for i in self.data if test(i)]
        self.assertEqual(len(result), 1)

    def test_search(self):
        """Test regex matching"""
        test = expr.name.search(r"^ja.*")
        result = [i for i in self.data if test(i)]
        self.assertEqual(len(result), 1)

    def test_filter(self):
        test = expr.score.filter(lambda x: x >= 55)
        result = [i for i in self.data if test(i)]
        self.assertEqual(len(result), 2)


if __name__ == "__main__":
    unittest.main()
