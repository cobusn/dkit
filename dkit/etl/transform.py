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
from . import schema
from ..parsers import infix_parser
from typing import Dict, Iterator, List
from abc import ABC, abstractmethod
import yaml
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


class Transform(ABC):

    def __iter__(self):
        for entry in self.the_iterable:
            yield entry

    @abstractmethod
    def __call__(self, the_iterable) -> Iterator[Dict]:
        yield from the_iterable


class FilterFieldsTransform(Transform):
    """yield only specified fields"""
    def __init__(self, fields_list: List[str]):
        self.fields_list = fields_list

    def __call__(self, the_iterable):
        fields = self.fields_list
        for row in the_iterable:
            yield {k: row[k] for k in fields}


class FormulaTransform(Transform):
    """
    Transform records using defined formula

    Args:
        rule_map: dictiony of field name, formula pairs
    """
    def __init__(self, rule_map):
        self.raw_recipe = rule_map
        self.recipe = {
            k: infix_parser.ExpressionParser(v.strip())
            for k, v in rule_map.items()
        }

    def transform(self, row):
        """transform one row"""
        return {k: p(row) for k, p in self.recipe.items()}

    def __call__(self, the_iterable):
        return (
            {k: p(row) for k, p in self.recipe.items()}
            for row in the_iterable
        )

    @classmethod
    def from_entity(cls, entity, key_case='same'):
        """
        Create transform from entity dictionary

        Args:
            entity: dictionary of entities
            case: upper, lower, same or camel
        """
        case_transforms = {
            'upper': lambda x: x.upper(),
            'lower': lambda x: x.lower(),
            'same': lambda x: x,
            'camel': lambda x: x.capitalize(),
        }
        case_fn = case_transforms[key_case]
        new_transform = {}
        for key in [i for i in entity if not i.startswith("__")]:
            new_transform[case_fn(key)] = "${{{}}}".format(key)
        return new_transform

    @classmethod
    def from_yaml(cls, yaml_text):
        rule_map = yaml.load(yaml_text, Loader=Loader)
        return cls(rule_map)


class CoerceTransform(Transform):
    """
    Coerce data to conform to specified model.

    Args:
        the_iterable: iterable of dict like objects
        the_validator: cetl.schema.SchemaValidator instance

    Yields:
        iterable of dict like objects that conform to specified schema

    """
    def __init__(self, the_validator):
        self.the_validator = the_validator

    def __call__(self, the_iterable):
        """
        coerce input rows to schema model.
        """
        map_py = schema.EntityValidator.map_python
        _schema = {
            k: map_py[v["type"]]
            for k, v in self.the_validator.schema.items()
        }
        for row in the_iterable:
            yield {
                k: v(row.get(k, None)) for k, v in _schema.items()
            }
