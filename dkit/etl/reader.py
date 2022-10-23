# Copyright (c) 2016 Cobus Nel
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

import bz2
import gzip
import io
import lzma
import re
import sys
import tarfile
import mmap
import logging
from . import DEFAULT_READ_CHUNK_SIZE
from ..utilities.cmd_helper import LazyLoad


posix_ipc = LazyLoad("posix_ipc")
logger = logging.getLogger(__name__)


class Reader():
    """
    Abstract base class for reader objects
    """
    pass


class OpenReader(Reader):
    """
    Abstract base class for open file readers
    """
    is_open = True


class EncodingStreamReader(OpenReader):

    def __init__(self, file_obj, encoding='utf-8'):
        self.file_obj = file_obj
        self.encoding = encoding

    def stream(self):
        return self

    def __iter__(self):
        encoding = self.encoding
        for line in self.file_obj:
            yield line.decode(encoding).strip()

    def readlines(self, hint):
        encoding = self.encoding
        return [i.decode(encoding) for i in self.file_obj.readlines(hint)]


class StreamReader(OpenReader):
    """
    stdin based reader
    """
    def __init__(self, file_object):
        self.file_obj = file_object

    def __iter__(self):
        yield from self.file_obj


class StdinReader(StreamReader):

    def __init__(self):
        super().__init__(sys.stdin)


class ClosedReader(Reader):
    is_open = False


class ArchiveReader(OpenReader):
    pass


class TarFileReader(ArchiveReader):
    """
    tar archive reader

    Reader for tar archives.  It is possible to specify filenames
    using a regular expression.

    :param file_name: file name
    :param regex: regular expression to select file names
    :param mode: mode for opening. Refer to python tarfile docs
    """
    def __init__(self, file_name, regex=r".*", mode="r:*", output_encoding="utf-8"):
        logger.info(f"reading from {file_name}")
        self.file_name = file_name
        self.regex = regex
        self.mode = mode
        self.encoding = output_encoding

    def __iter__(self):
        matcher = re.compile(self.regex)
        tar_obj = tarfile.open(self.file_name, self.mode, encoding="utf-8")
        for member_name in tar_obj.getnames():
            if matcher.match(member_name) is not None:
                # for line in tar_obj.extractfile(member_name):
                #    yield line.strip().decode(self.encoding)
                yield EncodingStreamReader(tar_obj.extractfile(member_name))


class FileReader(ClosedReader):

    def __init__(self, file_path, mode="r", encoding="utf-8"):
        logger.info(f"reading from {file_path}")
        self.file_path = file_path
        self.mode = mode
        self.encoding = encoding

    def open(self):
        if "b" in self.mode:
            return open(self.file_path, self.mode)
        else:
            return open(
                self.file_path,
                self.mode,
                encoding=self.encoding,
            )


class SharedMemoryReader(OpenReader):

    def __init__(self, name, compression=None):
        logger.info(f"reading from shared memory: {name}")
        self.name = name
        self.compresson = compression
        self.fd = posix_ipc.SharedMemory(
            name,
            posix_ipc.O_CREAT,
            read_only=True
        )
        self._buffer = mmap.mmap(
            self.fd.fd,
            length=self.fd.size,
            access=mmap.ACCESS_READ
        )

    def read(self, n):
        return self._buffer.read(n)

    def close(self):
        self._buffer.close()
        # self.fd.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class BufferedFileReader(ClosedReader):
    """
    Slightly optimised reader that utilize the mmap
    module
    """

    def __init__(self, file_path, mode="r", encoding="utf-8", chunk_size=DEFAULT_READ_CHUNK_SIZE):
        logger.info(f"reading from {file_path}")
        self.file_path = file_path
        self.mode = mode
        self.encoding = encoding
        self.file_obj = None
        self.chunk_size = chunk_size

    def open(self):
        if "b" in self.mode:
            # binary file
            self.file_obj = open(self.file_path, self.mode)
        else:
            # text file
            self.file_obj = open(self.file_path, self.mode, encoding=self.encoding)
        return self

    def __iter__(self):
        mm = mmap.mmap(self.file_obj.fileno(), self.chunk_size, flags=mmap.MAP_PRIVATE,
                       prot=mmap.PROT_READ)
        yield from mm.readline()

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.file_obj:
            self.file_obj.close()


class StringReader(ClosedReader):
    """
    Creates reader file object from string

    Args:
        * param: the_string: String object
        * io_type: file type (Default is io.StringIO)
    """
    def __init__(self, the_string, io_type=io.StringIO):
        self.io = io_type
        self.the_string = the_string
        self._buffer = self.io(self.the_string)

    def open(self):
        return self

    def __iter__(self):
        yield from self._buffer

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class BytesStringReader(StringReader):
    """
    return reader of type io.Bytestring

    useful for reading XML documents from string in
    memory.

    :param the_string: String object
    :param encoding: encoding, defualt to 'utf-8'
    """
    def __init__(self, the_string):
        super().__init__(the_string.encode('ascii'), io.BytesIO)


class CompressionReader(FileReader):

    def __init__(self, file_path, mode="rt", compression_lib=None):
        self.compression_lib = compression_lib
        super().__init__(file_path, mode)

    def open(self):
        return self.compression_lib.open(self.file_path, self.mode)


class Bz2Reader(CompressionReader):

    def __init__(self, file_path, mode="rt"):
        super().__init__(file_path, mode, compression_lib=bz2)


class LzmaReader(CompressionReader):

    def __init__(self, file_path, mode="rt"):
        super().__init__(file_path, mode, compression_lib=lzma)


class Lz4Reader(CompressionReader):

    def __init__(self, file_path, mode="rt"):
        import lz4.frame as lz4lib
        super().__init__(file_path, mode, compression_lib=lz4lib)


class GzipReader(CompressionReader):

    def __init__(self, file_path, mode="rt"):
        super().__init__(file_path, mode, compression_lib=gzip)
