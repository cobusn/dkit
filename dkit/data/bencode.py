# Copyright (c) 2020 Cobus Nel
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
Simple bencode implementation

Bencode is useful for computing an MD5 hash of a python data structure

This implementation support the following data types:
    * list
    * dict
    * tuple (not part of standard)
    * int
    * string
    * None

References:
    * https://en.wikipedia.org/wiki/Bencode
    * https://effbot.org/zone/bencode.htm

"""
import re

__all__ = [
    "encode",
    "decode",
]


def __encode(obj):
    """Encode to bencode"""
    enc_map = {
        type(None): lambda x: 'i-0e',
        int: lambda x: 'i%se' % x,
        str: lambda x: '%s:%s' % (len(x), x),
        list: lambda x: 'l%se' % ''.join(__encode(i) for i in x),
        tuple: lambda x: 't%se' % ''.join(__encode(i) for i in x),
        dict: lambda x: 'd%se' % ''.join(
            __encode(i) for kv in x.items() for i in kv
        ),
    }
    try:
        resp = enc_map[obj.__class__](obj)
    except ValueError:
        raise ValueError('type "%s" is not supported: %s' % (type(obj), obj))

    return resp


def encode(obj):
    return __encode(obj).encode("ascii")


def _tokenize(text, match=re.compile(r"([idelt])|(\d+):|(-?\d+)").match):
    i = 0
    while i < len(text):
        m = match(text, i)
        s = m.group(m.lastindex)
        i = m.end()
        if m.lastindex == 2:
            yield "s"
            yield text[i:i+int(s)]
            i = i + int(s)
        else:
            yield s


def _decode_item(parser, token):
    if token == "i":
        # integer: "i" value "e"
        data = next(parser)
        if data == "-0":
            data = None
        else:
            data = int(data)
        if next(parser) != "e":
            raise ValueError
    elif token == "s":
        # string: "s" value (virtual tokens)
        data = next(parser)
    elif token in ["l", "d", "t"]:
        # container: "l" (or "d") values "e"
        data = []
        tok = next(parser)
        while tok != "e":
            data.append(_decode_item(parser, tok))
            tok = next(parser)

        if token == "t":
            return tuple(data)
        if token == "d":
            data = dict(zip(data[0::2], data[1::2]))
    else:
        raise ValueError
    return data


def decode(text):
    try:
        src = _tokenize(text.decode('utf-8'))
        data = _decode_item(src, next(src))
        for token in src:  # look for more tokens
            raise SyntaxError("trailing junk")
    except (AttributeError, ValueError, StopIteration):
        raise SyntaxError("syntax error")
    return data
