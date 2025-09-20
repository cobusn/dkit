# Copyright (c) 2019 Cobus Nel

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
File Utilities

- FileLock: Use a file to lock a resource
- FileObjStub: use obj.open() to open files (for interchangeability with bz2)
- sanitise_name: strip unwanted characters from a filename
- yaml_load: helper to use the C Loader
-
"""
import os
import time
import errno
import tempfile
import pathlib
import yaml
from typing import Union, TextIO, Text
import re
from ..exceptions import DkitFileLockException
import builtins

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


__all__ = [
    "FileLock",
    "FileObjStub",
    "sanitise_name",
    "yaml_load",
]


class FileObjStub:
    """Mimics the bz2 interface but just uses normal open().

    simple stub that allow you to use normal files interchangably
    with interfaces such as bz2.

    e.g:
        ``` python
        fobj = FileObj (or bz2)
        f = fobj.open(filename)
        ```
    """
    @staticmethod
    def open(filename, mode="rt", *args, **kwargs):
        return builtins.open(filename, mode, *args, **kwargs)


def yaml_load(stream: Union[TextIO, Text]):
    """
    helper function parse yaml from text

    Args:
        - stream: file like object
    """
    return yaml.load(stream, Loader=Loader)


def temp_filename(root=None, suffix=None) -> pathlib.Path:
    """
    generate temporary filename

    arguments:
        root: root folder. use system default if none (/tmp)
        suffix: file suffix (optional)

    returns: <pathlib.Path>  object
    """
    _root = root if root else tempfile._get_default_tempdir()
    _fname = next(tempfile._get_candidate_names())
    if suffix is not None:
        _fname = _fname + "." + suffix
    retval = pathlib.Path(_root) / _fname
    return retval


def sanitise_name(file_name, sub="_"):
    """
    sanitize text to be suitable as filenames:

        -   change to lower case
        -   replace spaces with substitute
    """
    s = re.sub(r"[^\w\s]", sub, file_name.strip().lower())
    s = re.sub(r"\s+", '_', s)
    return s

#
# FileLock adapted from https://github.com/dmfrey/FileLock
#
# The license below applies
#
# Copyright (c) 2009, Evan Fosmark
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of the <organization> nor the
#       names of its contributors may be used to endorse or promote products
#       derived from this software without specific prior written permission.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


class FileLock(object):
    """file locking mechanism

    A file locking mechanism that has context-manager support so
    you can use it in a with statement. This should be relatively cross
    compatible as it doesn't rely on msvcrt or fcntl for the locking.
    """
    def __init__(self, file_name, timeout=10, delay=.05):
        """
        Prepare the file locker. Specify the file to lock and optionally
        the maximum timeout and the delay between each attempt to lock.
        """
        if timeout is not None and delay is None:
            raise ValueError("If timeout is not None, then delay must not be None.")
        self.is_locked = False
        self.lockfile = os.path.join(os.getcwd(), "%s.lock" % file_name)
        self.file_name = file_name
        self.timeout = timeout
        self.delay = delay

    def acquire(self):
        """
        Acquire the lock, if possible. If the lock is in use, it check again
        every `wait` seconds. It does this until it either gets the lock or
        exceeds `timeout` number of seconds, in which case it throws
        an exception.
        """
        start_time = time.time()
        while True:
            try:
                self.fd = os.open(self.lockfile, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                self.is_locked = True  # moved to ensure tag only when locked
                break

            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
                if self.timeout is None:
                    raise DkitFileLockException(
                        "Could not acquire lock on {}".format(self.file_name)
                    )
                if (time.time() - start_time) >= self.timeout:
                    raise DkitFileLockException("Timeout occured.")
                time.sleep(self.delay)

    def release(self):
        """
        Get rid of the lock by deleting the lockfile.
        When working in a `with` statement, this gets automatically
        called at the end.
        """
        if self.is_locked:
            os.close(self.fd)
            os.unlink(self.lockfile)
            self.is_locked = False

    def __enter__(self):
        """
        Activated when used in the with statement.
        Should automatically acquire a lock to be used in the with block.
        """
        if not self.is_locked:
            self.acquire()
        return self

    def __exit__(self, type, value, traceback):
        """
        Activated at the end of the with statement.
        It automatically releases the lock if it isn't locked.
        """
        if self.is_locked:
            self.release()

    def __del__(self):
        """
        Make sure that the FileLock instance doesn't leave a lockfile
        lying around.
        """
        self.release()
