#
# Copyright (C) 2017  Cobus Nel
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
from datetime import date, datetime, timedelta, timezone
from typing import Union, Iterable
import time
from ..typing import AnyDate


def daterange(begin: AnyDate, end: AnyDate,
              delta: timedelta = timedelta(1)) -> Iterable[AnyDate]:
    """generator that iterate over a set of dates

    Arguments:
        begin: a date (or datetime) object; the beginning of the range.
        end: a date (or datetime) object; the end of the range.
        delta: (optional) a timedelta object; how much to step each iteration.
                Default step is 1 day.
    """
    while begin < end:
        yield begin
        begin += delta


def daterange_pairs(begin, end, delta=timedelta(1)):
    """generator that iterate over a set of dates

    Arguments:
        begin: a date (or datetime) object; the beginning of the range.
        end: a date (or datetime) object; the end of the range.
        delta: (optional) a timedelta object; how much to step each iteration.
                Default step is 1 day.

    Returns:
        tuple with start, end dates
    """
    while begin < end:
        yield (begin, begin + delta)
        begin += delta


def hms(secs):
    """
    Convert seconds to Hours, Minutes, Seconds and Milli-Seconds

    returns (hour, minutes, seconds, milliseconds)

    :rtype: (int,int,int,int)
    """
    _hours = secs / 3600
    _mins = secs % 3600
    mins = int(_mins / 60)
    _secs = _mins % 60
    ms = _secs % 1 * 1000

    return int(_hours), mins, int(_secs), int(round(ms))


def local_tz_offset():
    """
    Local system timezone offset in hours
    """
    if time.daylight:
        offset_hour = time.altzone / 3600
    else:
        offset_hour = time.timezone / 3600
    return timezone(timedelta(hours=offset_hour))


# Local timezone offset as datetime.timezone instance
LOCAL_TZ_OFFSET = local_tz_offset()


def to_unixtime(the_time):
    """
    converts datetime to unix timestamp (epoch)

    Ars:
        the_time  datetime instance

    Returns:
        unix timestamp (long) of provided datetime
    """
    return the_time.timestamp()


def first_day_of_month(the_date: AnyDate) -> date:
    """
    returns date of first day of month
    """
    return date(the_date.year, the_date.month, 1)


def datestamp(the_date: AnyDate) -> int:
    """
    timestamp of day (midnight)

    args:
        - date
    """
    d = date(the_date.year, the_date.month, the_date.day)
    return int(d.strftime("%s"))


def to_datetime(the_date: date) -> datetime:
    """
    convert date to datetime
    """
    return datetime(the_date.year, the_date.month, the_date.day)


def from_unixtime(the_timestamp, tz_offset=LOCAL_TZ_OFFSET):
    """
    convert from unix timestamp to datetime

    This function is timezone aware

    Args:
        the_timestamp: unix timestamp (long)
        tz_offset: offset in hours. Defaults to local offset

    Returns:
        datetime
    """
    return datetime.fromtimestamp(the_timestamp, tz_offset)


def fmt_std(the_date: datetime) -> str:
    """
    string format YYYY-DD-MM HH:MM:SS
    """
    return the_date.strftime("%Y-%m-%d %H:%M:%S")
