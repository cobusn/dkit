#
# Copyright (C) 2015  Cobus Nel
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#
from uuid import uuid4
from zlib import crc32, adler32


URLSAFE32 = "abcdefghijkmnpqrstuvwxy23456789"
URLSAVE32L = "ABCDEFGHIJKMNPQRSTUVWXY23456789"
A85CHARSET = "!#$%&()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[]^_abcdefghijklmnopqrstu"
SENSIBLE = "abcdefghijkmnpqrstuvwxyABCDEFGHIJKMNPQRSTUVWXY23456789"


def short_crc32(data, alphabet=SENSIBLE):
    """crc32 encoded with alphabet to shorten string"""
    return encode(crc32(data), SENSIBLE)


def short_adler32(data, alphabet=SENSIBLE):
    """adler hash encoded with alphabet to shorten string

    adler is faster than crc32
    """
    return encode(adler32(data), SENSIBLE)


def uid():
    """Short unique identifier"""
    return encode(hash(uuid4()), alphabet=A85CHARSET)


def encode(num, alphabet=URLSAFE32):
    """
    encode a positive number in Base X

    Arguments:
    - `num`: The number to encode
    - `alphabet`: The alphabet to use for encoding
    """
    if num == 0:
        return alphabet[0]
    arr = []
    base = len(alphabet)
    while num:
        num, rem = divmod(num, base)
        arr.append(alphabet[rem])
    arr.reverse()
    return ''.join(arr)


def decode(string, alphabet=URLSAFE32):
    """
    Decode a Base X encoded string to number
    Arguments:
    - `string`: The encoded string
    - `alphabet`: The alphabet to use for encoding
    """
    base = len(alphabet)
    strlen = len(string)
    num = 0

    idx = 0
    for char in string:
        power = (strlen - (idx + 1))
        num += alphabet.index(char) * (base ** power)
        idx += 1

    return num
