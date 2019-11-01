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

"""
Parsing utilities
"""

import re
from ..exceptions import CkitParseException


class AbstractScanner(object):
    def __init__(self, rules, fn_remainder=None, break_on_match=True):
        self.break_on_match = break_on_match
        self.rules = rules
        self.compiled = [(re.compile(r), fn) for r, fn in rules]
        self.fn_remainder = fn_remainder

    def scan(self, line):
        raise NotImplementedError    # pragma: no cover


class MatchScanner(AbstractScanner):
    """
    Scanner that will only match first part of string

    :rules: list of parsing rules with callbacks
    :fn_remainder: callback for remaining string
    :break_on_match: break on first match if set to True
    """

    def scan(self, line):
        """
        scan line of text
        """
        ln = line
        for rule, callback in self.compiled:
            result = rule.match(ln)
            if result is not None:
                callback(result)
                if self.break_on_match:
                    break
                ln = rule.sub('', ln).strip()
        if self.fn_remainder is not None:
            self.fn_remainder(ln)


class SearchScanner(AbstractScanner):
    """
    Scanner that will match any part of string

    .. warning:: Note the limitation n the current implementaton that
                 that each token is scanned only once.

    :rules: list of parsing rules with callbacks
    :fn_remainder: callback for remaining string
    :break_on_match: break on first match if set to True

    """

    def scan(self, line):
        ln = line
        for rule, callback in self.compiled:
            result = rule.search(ln)
            if result is not None:
                callback(result)
                if self.break_on_match:
                    break
                ln = ln[:result.start()] + ln[result.end() + 1:]
        if self.fn_remainder is not None:
            self.fn_remainder(ln)


#
# the following functions/classes are used by infix_parser
#
def rex_match_closure():
    def validate_fn(parser, strg, tokens):
        fname = tokens[0]
        if len(tokens) != 3:
            raise CkitParseException(f"function {fname} require 2 parameters")
        expression = tokens[2]
        scanner = re.compile(expression)

        # The regex will be the last item on the parse stack. Since
        # this is in the closure we need to remove it from the parse
        # stack..
        parser._parse_stack.pop()

        def match(parser):
            return scanner.search(parser._internal_eval()) is not None
        return match
    return validate_fn


class deprecated_RegexMatch(object):
    """
    deprecated in favor of closure version

    pre compiled regular expression class that act as a function

    used by infix_parser as `rex`
    """
    def __init__(self, strg, tokens):
        if len(tokens) != 3:
            raise CkitParseException(f"RegexMatch require 2 paramters, provided: {strg}")
        expression = tokens[2]
        self.scanner = re.compile(expression)

    def __call__(self, target):
        return self.scanner.search(target) is not None
