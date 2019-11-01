# Copyright (c) 2018 Cobus Nel
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

from html import parser


class HTMLTableParser(parser.HTMLParser):
    """
    parse html tables

    >>> input = "<table><tr><th>name</th></tr><tr><td>sam</td></tr></table>"
    >>> p = HTMLTableParser().feed(input)
    >>> print(p.data)
    [['sam']]
    >>> print(p.headings)
    ['name']

    The process method return a generator:

    >>> print(list(p.process(input)))
    [{'name': 'sam'}]

    """
    def __init__(self):
        super().__init__()
        self.data = []
        self._init_data()

    def _init_data(self):
        self.in_data = False
        self.in_header = False
        self.current_data = None
        self.current_row = []
        self.headings = []
        self.processing_headings = True
        self.data = []

    def handle_starttag(self, tag, attrs):
        """entering a tag"""
        if tag == 'td':
            self.in_data = True
        elif tag == "th":
            self.in_header = True

    def handle_data(self, data):
        """parse data"""
        if self.in_data:
            self.current_data = data.strip()
        elif self.in_header:
            self.headings.append(data.strip())

    def handle_endtag(self, tag):
        """
        processing when end tag is encountered
        """
        if tag == "th":
            self.in_header = False
        elif tag == "td":
            self.current_row.append(self.current_data)
            self.in_data = False
        elif tag == "tr" and self.lasttag != "th":
            self.data.append(self.current_row)
            self.current_row = []

    def process(self, data):
        """
        generator for records in table
        """
        self.feed(data)
        if len(self.headings) == 0:
            for row in self.data:
                yield {i: v for i, v in enumerate(row)}
        else:
            for row in self.data:
                yield dict(zip(self.headings, row))

    def feed(self, data):
        self._init_data()
        super().feed(data)
        return self
