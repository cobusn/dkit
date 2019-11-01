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
"""
Convenience functions and classes dealing with logging
"""
import logging
import sys

class LegacyNullHandler(logging.Handler):
    """
    Null Handler used included as this is not available in
    Python versions < 2.7.
    """
    def handle(self, record):
        pass

    def emit(self, record):
        pass

    def createLock(self):
        self.lock = None

def null_logger(name='msutils.null_logger'):
    """
    Return logger that does nothing.
    """
    the_logger = logging.getLogger(name)
    the_logger.addHandler(LegacyNullHandler())
    return the_logger

def _get_logger(handler, logger_name, template, level):
    the_logger = logging.getLogger(logger_name)
    formatter = logging.Formatter(template)
    handler.setFormatter(formatter)
    the_logger.addHandler(handler)
    the_logger.setLevel(level)
    return the_logger

def file_logger(filename, logger_name='file_logger', template='%(created)f %(message)s', level=logging.INFO):
    """
    Return simple file logger
    """
    handler = logging.FileHandler(filename)
    return _get_logger(handler, logger_name, template, level)

def stream_logger(stream, logger_name='stream_logger', template="%(message)s", level=logging.INFO):
    """
    Return stream logger
    """
    handler = logging.StreamHandler(stream)
    return _get_logger(handler, logger_name, template, level)

def stderr_logger(logger_name='stderr_logger', template="%(message)s", level=logging.INFO):
    """
    Return stream logger to stderr
    """
    return stream_logger(sys.stderr, logger_name, template, level)

def stdout_logger(logger_name='stdout_logger', template="%(message)s", level=logging.INFO):
    """
    Return stream logger to stdout
    """
    return stream_logger(sys.stdout, logger_name, template, level)
