import os
import struct
from builtins import open as _builtin_open


"""
inspired by Interchange File Format
"""


class IFFWriter(object):

    def __init__(self, filename):
        if isinstance(filename, (str, os.PathLike)):
            self._fp = _builtin_open(filename, "wb")
        elif hasattr(filename, "read") or hasattr(filename, "write"):
            self._fp = filename

    def write(self, data):
        length = len(data)
        self._fp.write(struct.pack("@l", length))
        self._fp.write(data)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._fp:
            self.file.close()


class IFFReader(object):

    def __init__(self, filename):
        if isinstance(filename, (str, os.PathLike)):
            self._fp = _builtin_open(filename, "rb")
        elif hasattr(filename, "read") or hasattr(filename, "write"):
            self._fp = filename

    def read(self):
        packed = self._fp.read(8)
        if len(packed) == 0:
            raise EOFError()
        chunk_len = struct.unpack("@l", packed)[0]
        return self._fp.read(chunk_len)

    def seek(self, n):
        self._fp.seek(n)

    def __iter__(self):
        while True:
            try:
                yield self.read()
            except EOFError:
                break

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._fp:
            self.file.close()
