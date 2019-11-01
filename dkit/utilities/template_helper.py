# Copyright (c) 2018 Cobus Nel
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
helper functions for working with jinja templates
"""
from ..data import containers
from . import time_helper, intervals
import datetime
import dateutil


def template_macros(**kwargs):
    """
    provide set of commonly used functions to template.

    Returns:
        Dict
    """
    macros = containers.AttrDict()
    classes = {
        "datetime": datetime.datetime,
        "timedelta": datetime.timedelta,
        "dateutil": dateutil,
        "time": datetime.time,
        "to_unixtime": time_helper.to_unixtime,
        "from_unixtime": time_helper.from_unixtime,
        "to_datetime": time_helper.to_datetime
    }

    macros.update(intervals.DATE_MAP)
    macros.update(classes)
    return macros
