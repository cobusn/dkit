"""text processing utilities"""

from io import StringIO
from html.parser import HTMLParser

all = [
    "strip_tags"
]


# https://stackoverflow.com/questions/753052/strip-html-from-strings-in-python
class __MLStripper(HTMLParser):
    """strip html tags"""
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs = True
        self.text = StringIO()

    def handle_data(self, d):
        self.text.write(d)

    def get_data(self):
        return self.text.getvalue()


def strip_tags(html):
    """strip out all html tags"""
    s = __MLStripper()
    s.feed(html)
    return s.get_data()
