import unittest
import sys
sys.path.insert(0, "..")  # noqa
from dkit.parsers.helpers import parse_kv_pairs


class TestParserHelper(unittest.TestCase):

    def test_kv_parser_1(self):
        t1 = 'key1=value1,key2=\'value2,still_value2,not_key1="not_value1"\''
        d1 = parse_kv_pairs(t1)
        self.assertEqual(
            {'key1': 'value1', 'key2': 'value2,still_value2,not_key1="not_value1"'},
            d1
        )

    def test_kv_parser_2(self):
        t1 = "k1=10,k2='how am i, doing'"
        d1 = parse_kv_pairs(t1)
        self.assertEqual(
            d1,
            {
                'k1': '10',
                'k2': 'how am i, doing'
            }
        )


if __name__ == '__main__':
    unittest.main()
