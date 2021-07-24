"""
Simple parameter parser for command line
use.

e.g x=10
"""
from typing import List, Dict


def _clean(str_input: str):
    str_input = str_input.strip()
    str_input = str_input.replace('"', "")
    str_input = str_input.replace("'", "")
    return str_input


def parse_parameter(str_input: str):
    spl = tuple(_clean(i) for i in str_input.split("="))
    return spl


def parameter_dict(lst_input: List) -> Dict:
    return dict(parse_parameter(i) for i in lst_input)


if __name__ == "__main__":
    print(parse_parameter("a=10"))
    print(parameter_dict(["a=10", 'b="james"', 'c="2012-12-12']))
