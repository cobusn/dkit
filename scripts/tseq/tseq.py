#!/usr/bin/env python

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

import sys
import argparse
from dateutil.relativedelta import relativedelta
from datetime import datetime
from datetime import date

DEFAULT_OF = "iso_date"
DEFAULT_IF = "iso_date"
CHOICES = ["years", "months", "days", "minutes", "seconds"]
DEFAULT_TYPE = "days"
FORMATS = ["iso", "iso_day", "epoch", "strftime format"]
DESCRIPTION = """tseq - print a sequence of date/time values"""


class TimeSequence(object):

    def __init__(self, start, stop, kind="month", inc=1, pairs=False):
        self.start = start
        self.stop = stop
        if kind not in CHOICES:
            raise ValueError(f"kind should be one of {', '.join(CHOICES)}")
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

    def __iter__(self):
        incr = relativedelta(**{self.kind: self.inc})
        d = self.start
        while d < self.stop:
            if self.pairs:
                yield d, d + incr
            else:
                yield d
            d = d + incr


def run(args):
    in_format = args.ifmt
    out_format = args.ofmt
    start = TimeSequence.parse(args.start, in_format)
    stop = TimeSequence.parse(args.stop, in_format)

    t_it = TimeSequence(start, stop, args.type, pairs=args.pairs)

    for t in t_it:
        if args.pairs:
            _start, _stop = t
            sys.stdout.write(t_it.format(_start, out_format))
            sys.stdout.write(args.fs)
            sys.stdout.write(t_it.format(_stop, out_format))
        else:
            sys.stdout.write(t_it.format(t, out_format))
        sys.stdout.write("\n")


def main():
    description = DESCRIPTION
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument(
        "start",
        help="start time (format specified by --ifmt)"
    )
    parser.add_argument(
        "stop",
        help="exclusive end time (format specified by --ifmt)"
    )
    parser.add_argument(
        '--fs', default=" ",
        help="field separator for pairs"
    )
    parser.add_argument(
        '-p', '--pairs', action="store_true", default=False,
        help="output as subsequent pairs"
    )
    parser.add_argument(
        "--ifmt",  default=DEFAULT_IF,
        help=f"input format ({', '.join(FORMATS)})"
    )
    parser.add_argument(
        "--ofmt", default=DEFAULT_OF,
        help=f"output format ({', '.join(FORMATS)})"
    )
    parser.add_argument(
        '-t', '--type',
        choices=CHOICES,
        default=DEFAULT_TYPE,
        help="output type"
    )
    run(parser.parse_args())


if __name__ == "__main__":
    main()
