# Copyright (c) 2019 Cobus Nel
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
Convenience functions and classes dealing with logging
"""
import logging
import sys


def init_logger(message=None, name=None, level=logging.DEBUG, handler=None):
    """
    generic logger initialization function
    """
    logger = logging.getLogger(name)
    _message = message or '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
    formatter = logging.Formatter(_message)
    handler = handler or logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)


def init_null_logger(message=None, name=None, level=logging.DEBUG):
    """
    logger that does nothing
    """
    logger = logging.getLogger(name)
    logger.addHandler(logging.NullHandler())


def init_file_logger(filename, message=None, name=None, level=logging.DEBUG):
    """
    Return simple file logger
    """
    handler = logging.FileHandler(filename)
    init_logger(message, name, level, handler)


def init_stream_logger(stream, message=None, name=None, level=logging.DEBUG):
    """
    Return stream logger
    """
    handler = logging.StreamHandler(stream)
    init_logger(message, name, level, handler)


def init_stderr_logger(message=None, name=None, level=logging.DEBUG):
    """
    Return stream logger to stderr
    """
    init_stream_logger(sys.stderr, message=None, name=None, level=logging.DEBUG)


def init_stdout_logger(message=None, name=None, level=logging.DEBUG):
    """
    Return stream logger to stderr
    """
    init_stream_logger(sys.stdout, message=None, name=None, level=logging.DEBUG)
