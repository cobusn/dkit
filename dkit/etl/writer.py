#
# Copyright (C) 2016  Cobus Nel
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

# Note import of hdfs library in class definition.
import sys
import bz2
import codecs
import lzma
import gzip
import posix_ipc
import io
import mmap
from ..exceptions import CkitETLException


class Writer(object):
    """
    Base class for writers
    """
    pass


class OpenWriter(Writer):
    is_open = True


class ClosedWriter(Writer):
    is_open = False


class StreamWriter(OpenWriter):
    """
    Stream Destination
    """
    def __init__(self, stream):
        self._stream = stream

    @property
    def stream(self):
        return self._stream


class StdOutWriter(StreamWriter):

    def __init__(self):
        super().__init__(sys.stdout)


class FileWriter(ClosedWriter):
    """
    File Destination.

    :param path: file path
    :param mode: file mode
    :param encoding: file encoding="utf-8"
    :param errors: encoding errors = "surrogateescape"
    """
    def __init__(self, path, mode="w", encoding="utf-8", errors="surrogateescape"):
        self.path = path
        self.mode = mode
        self.encoding = encoding
        self.errors = errors

    def open(self):
        if "b" in self.mode:
            return open(self.path, self.mode)
        else:
            return open(self.path, self.mode, encoding=self.encoding, errors=self.errors)


class SharedMemoryWriter(OpenWriter):

    def __init__(self, file_name, compression=None):
        self.file_name = file_name
        self._buffer = io.BytesIO()
        self.flushed = False

    def __del__(self):
        if not self.flushed:
            self.close()

    def write(self, b):
        self._buffer.write(b)

    def flush(self):
        if self.flushed:
            raise CkitETLException(
                f"{self.__class__.__name__} data can only be flushed once."
            )
        self.flushed = True
        size = self._buffer.getbuffer().nbytes
        fd = posix_ipc.SharedMemory(
            self.file_name,
            posix_ipc.O_CREAT,
            size=size
        )
        w = mmap.mmap(fd.fd, length=fd.size)
        self._buffer.seek(0)
        w.write(self._buffer.read())
        w.close()

    def close(self):
        self.flush()

    def open(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class CodecWriter(FileWriter):

    def __init__(self, path, mode="w", codec="utf-8"):
        super().__init__(path, mode)
        self.codec = codec

    def open(self):
        return codecs.open(self.path, self.mode, self.codec)


class CompressionWriter(FileWriter):

    def __init__(self, path, mode="wt", compression_lib=None):
        self.compression_lib = compression_lib
        super().__init__(path, mode)

    def open(self):
        return self.compression_lib.open(self.path, self.mode)


class Bz2Writer(CompressionWriter):
    """
    Write to bzip2
    """
    def __init__(self, path, mode="wt"):
        super().__init__(path, mode, bz2)


class LzmaWriter(CompressionWriter):
    """
    Write to .xz files
    """
    def __init__(self, path, mode="wt"):
        super().__init__(path, mode, lzma)


class Lz4Writer(CompressionWriter):
    """
    Write to .lz4 files
    """
    def __init__(self, path, mode="wt"):
        import lz4.frame as lz4lib
        super().__init__(path, mode, lz4lib)


class GzipWriter(CompressionWriter):
    """
    Write to zip files
    """
    def __init__(self, path, mode="wt"):
        super().__init__(path, mode, gzip)
