# Copyright (c) 2017 Cobus Nel
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import datetime
import decimal
import re
from functools import partial

from . import helpers
from ..data.helpers import to_boolean
from ..exceptions import DKitParseException


re_bool = r"True|False"

#
# NB: Edit dkit/etl/schema.py as well when changing the
# below entries. In addition:
#
# - ext_arrow
# - ext_sql_alchemy
# - ext_athena
# - ext_avro
# - ext_spark
#
type_map = {
    "Binary": bytes,
    "Boolean": bool,
    "Date": datetime.date,
    "DateTime": datetime.datetime,
    "Decimal": decimal.Decimal,
    "Double": float,
    "Float": float,
    "Int16": int,
    "Int32": int,
    "Int64": int,
    "Int8": int,
    "UInt16": int,
    "UInt32": int,
    "UInt64": int,
    "UInt8": int,
    "Integer": int,
    "String": str,
    "Time": datetime.time,
}


class TypeParser(object):

    parameter_map = {
        "str_len": [r"\d+", int],
        "precision": [r"\d+", int],
        "scale": [r"\d+", int],
        "primary_key": [re_bool, to_boolean],
        "unique": [re_bool, to_boolean],
        "index": [re_bool, to_boolean],
        "autoincrement": [re_bool, to_boolean],
        "computed": [re_bool, to_boolean],
        "nullable": [re_bool, to_boolean],
        "info": [r"\w+", str],
    }

    types_ = r"|".join([i for i in type_map])

    def __init__(self):
        self.data = {}
        rules = []
        for k, v in self.parameter_map.items():
            rules.append(
                (r"({})=({})".format(k, v[0]), partial(self.parse_value_pair, converter=v[1]))
            )
        self.parser = helpers.SearchScanner(rules, self.process_remainder, False)

    def parse(self, the_text):
        self.data = {}
        re_string = r"\s*({})\((.*)\)".format(self.types_)
        m = re.match(re_string, the_text)
        if m is not None:
            g = m.groups()
            self.data["type"] = g[0].lower()
            params = g[1].strip()
            if len(params) > 0:
                self.parser.scan(g[1])
        else:
            raise DKitParseException(f"type: '{the_text}' cannot be parsed")
        return self.data

    def parse_value_pair(self, result, converter=str):
        g = result.groups()
        self.data[g[0]] = converter(str(g[1]))

    def process_remainder(self, remainder):
        """
        test remaining text for invalid content
        """
        remainder = remainder.strip(", ")
        if len(remainder) > 0:
            raise ValueError(remainder)
