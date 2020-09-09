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
Verify if data have been processed.
"""
import shelve
import time
import logging
from dkit.utilities import instrumentation


logger = logging.getLogger(__name__)


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
    """
    def __init__(self, file_name: str, getter, flag="c"):
        self.file_name = file_name
        self.getter = getter
        self.db = shelve.open(file_name, flag=flag)
        self.stats = instrumentation.CounterLogger(self.__class__.__name__).start()

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
                logger.info("{} not completed. processing..".format(key))
                yield row
            else:
                logger.info("{} completed, skipping".format(key))

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
