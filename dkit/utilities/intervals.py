
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
Interval creation utilities

=========== =============== =================================================
Sep 2019    Cobus Nel       Created
=========== =============== =================================================

"""
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from typing import Tuple


OFFSET_MAP = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6
}


DATE_MAP = {
    "monday": lambda: weekday("monday"),
    "tuesday": lambda: weekday("tuesday"),
    "wednesday": lambda: weekday("wednesday"),
    "thursday": lambda: weekday("thursday"),
    "friday": lambda: weekday("friday"),
    "saturday": lambda: weekday("saturday"),
    "sunday": lambda: weekday("sunday"),
    #
    "today": lambda: today(),
    "tomorrow": lambda: tomorrow(),
    "last_month": lambda: last_month(),
    "next_week": lambda: next_week(),
    "next_month": lambda: next_month(),
    "this_week": lambda: this_week(),
    "this_month": lambda: last_month(),
    "yesterday": lambda: yesterday(),
}


def today():
    """
    todays datetime

    Returns:
        (today, today + 1 day)
    """
    tday = date.today()
    return (tday, tday + timedelta(days=1))


def tomorrow():
    """
    tomorrows datetime

    Returns:
        (tomorrow, tomorow + 1 day)
    """
    tday = date.today()
    return (tday + timedelta(days=1), tday + timedelta(days=2))


def last_month():
    """
    interval for this month

    Returns:
        (1st of this month, 1st of next month)
    """
    today = date.today()
    first_this_month = date(today.year, today.month, 1)
    return (first_this_month - relativedelta(months=1), first_this_month)


def this_month():
    """
    interval for this month

    Returns:
        (1st of this month, 1st of next month)
    """
    today = date.today()
    first_this_month = date(today.year, today.month, 1)
    return (first_this_month, first_this_month + relativedelta(months=1))


def next_month():
    """
    interval for next month

    Returns:
        (1st of next month, 1st of 2 months from now)
    """
    today = date.today()
    first_this_month = date(today.year, today.month, 1)
    return (first_this_month + relativedelta(months=1), first_this_month + relativedelta(months=2))


def next_week():
    """
    datetime interval for next week

    Returns:
        (next week, next week + 1 week)
    """
    td = date.today()
    nw = td + timedelta(days=7-td.weekday())
    return (nw, nw + timedelta(days=7))


def this_week():
    """
    return dates for next week monday as datetime.date

    Returns:
        (monday, next monday)
    """
    tday = date.today()
    monday = tday + timedelta(days=-tday.weekday())
    return(monday, monday + timedelta(days=7))


def yesterday() -> Tuple[date, date]:
    """
    :returns: tuple({yesterdays date}, {todays date})
    """
    tday = date.today()
    return (tday - timedelta(days=1), tday)


def weekday(weekday):
    """
    return date for weekday specified
    """
    tday = date.today()
    wkday = tday + timedelta(days=(-tday.weekday() + OFFSET_MAP[weekday]))
    return (wkday, wkday + timedelta(days=1))


def month(the_date):
    """
    returns tuple({first day of month, first the of the next month})
    """
    d_start = date(the_date.year, the_date.month, 1)
    return (d_start, d_start + relativedelta(months=1))
