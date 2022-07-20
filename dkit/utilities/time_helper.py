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
from typing import Iterable
import time
from typing import AnyDate, Iterator
from dateutil.relativedelta import relativedelta


class TimeSequence(object):
    """
    tseq - print a sequence of date/time values

    useful for generating parameters for batch
    programs

    args:
        - start: start date/time
        - stop: stop date/time
        - kind: jump size (e.g. days)
        - inc: increment size
        - pairs: yield pairs if True

    """

    CHOICES = ["years", "months", "days", "minutes", "seconds"]

    def __init__(self, start: datetime, stop: datetime, kind: str = "month",
                 inc: int = 1, pairs: bool = False):
        self.start = start
        self.stop = stop
        if kind not in self.CHOICES:
            raise ValueError(f"kind should be one of {', '.join(self.CHOICES)}")
        self.kind = kind
        self.pairs = pairs
        self.inc = inc

    @staticmethod
    def __get_formatter(spec):
        if spec == "iso_date":
            return lambda x: date(x.year, x.month, x.day).isoformat()
        elif spec == "iso":
            return lambda x: x.isoformat(" ")
        elif spec == "epoch":
            return lambda x: str(int(x.timestamp()))
        else:
            return lambda x: x.strftime(spec)

    @staticmethod
    def parse(value, spec):
        """
        convenience function to parse a date

        spec can be any of:
            - iso_date
            - iso
            - epoch
            - custom strptime format
        """
        translate = {
            'today': lambda: date.today(),
            'tomorrow': lambda: date.today() + relativedelta(days=1),
            'yesterday': lambda: date.today() - relativedelta(days=1)
        }
        if value in translate:
            d = translate[value]()
            return datetime(d.year, d.month, d.day)

        if spec == "iso_date":
            return datetime.strptime(value, "%Y-%m-%d")
        elif spec == "iso":
            return datetime.fromisoformat(value)
        elif spec == "epoch":
            return datetime.fromtimestamp(int(value))
        else:
            return datetime.strptime(value, spec)

    def format(self, obj, format_spec):
        return TimeSequence.__get_formatter(format_spec)(obj)

    def __iter__(self) -> Iterator[datetime]:
        incr = relativedelta(**{self.kind: self.inc})
        d = self.start
        while d < self.stop:
            if self.pairs:
                yield d, d + incr
            else:
                yield d
            d = d + incr


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


def hourstamp(the_date: datetime) -> int:
    """
    timestamp of day (midnight)

    args:
        - date
    """
    d = datetime(the_date.year, the_date.month, the_date.day, the_date.hour)
    return int(d.strftime("%s"))


def hour_of_day(the_date: AnyDate) -> int:
    return int(the_date.strftime("%H"))


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


def day_id_from_int(timestamp: int):
    """day identifier from unix timestamp

    useful for database keys

    args:
        - timestamp: unix timestamp

    returns:
        timestamp of the day
    """
    ts = int(timestamp)
    return ts - (ts % 86400)


def hour_id_from_int(timestamp):
    """day identifier from unix timestamp

    useful for database keys

    args:
        - timestamp: unix timestamp

    returns:
        timestamp of the day
    """
    ts = int(timestamp)
    return ts - (ts % 3600)


def hour_of_day_from_int(timestamp: int, tz_adjust=0):
    """
    hour in day
    """
    return int(
        (hour_id_from_int(timestamp) - day_id_from_int(timestamp)) / 3600
    )
