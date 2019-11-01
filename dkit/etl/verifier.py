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
Verify if data have been processed.
"""
import shelve
import time
from dkit.utilities import instrumentation
from dkit.utilities import log_helper as log


class VerifierRecord(object):

    def __init__(self, timestamp=None):
        self.timestamp = timestamp if timestamp else time.time()


class ShelveVerifier(object):
    """
    task completion verifier

    Args:
        * file_name: database filename
        * getter: key getter (e.g. lambda function)
        * flag: flag (refer to shelve documentation)
        * logger: logger
    """
    def __init__(self, file_name: str, getter, flag="c", logger=None):
        self.file_name = file_name
        self.getter = getter
        self.db = shelve.open(file_name, flag=flag)
        self.logger = logger if logger else log.null_logger()
        self.stats = instrumentation.CounterLogger(logger=self.logger).start()

    def __del__(self):
        self.db.close()

    def test_completed(self, key):
        """Test if one item is completed"""
        if key is not None and key in self.db:
            return True
        else:
            return False

    def iter_not_completed(self, the_iterator, getter=None):
        """Iterator that only allow items not completed"""
        get_key = getter if getter else self.getter
        for row in the_iterator:
            key = get_key(row)
            if not self.test_completed(key):
                self.logger.info("{} not completed. processing..".format(key))
                yield row
            else:
                self.logger.info("{} completed, skipping".format(key))

    def iter_mark_as_complete(self, the_iterator, getter=None):
        """
        Ignore completed items, mark new items as complete and
        yield the row
        """
        get_key = getter if getter else self.getter
        for row in the_iterator:
            key = get_key(row)
            if not self.test_completed(key):
                self.mark_as_complete(key)
                yield row

    def mark_as_complete(self, key, value=VerifierRecord()):
        """
        mark row as completed

        Used internally by api.
        """
        if key is not None:
            self.db[key] = value
            self.stats.increment()
