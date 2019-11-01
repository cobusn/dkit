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
Convert Python profiler statistics to Json
"""
import argparse
from pstats import Stats
from dkit.etl.utilities import sink_factory
from dkit.parsers.uri_parser import parse


class PstatExport(object):
    """
    Python profiler stats data export utility

    args:
        *file_names: names of pstat files
    """
    def __init__(self, *file_names):
        self.file_names = file_names
        self.stat = Stats(*file_names)

    def iter_stats(self):
        """extract rows"""
        i = 0
        for k, v in self.stat.stats.items():
            cc, nc, tt, ct, callers = v
            if i == 2:
                # stderr.write(str(callers))
                pass

            i += 1
            row = {
                "hash": hash(k),
                "file": k[0],
                "line_no": k[1],
                "context": k[2],
                "n_calls": nc,
                "c_calls": cc,
                "total_time": tt,
                "time_per_call": 0 if nc == 0 else tt/nc,
                "cum_time": ct,
                "no_callers": len(callers),
            }
            yield row


def main():
    """main appliction logic"""
    description = "Export pstats data to other formats"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('files', metavar='GLOB', nargs='+', help='files')
    parser.add_argument('-o', dest="output", help='output uri. default to jsonl on stdio',
                        default="jsonl:///stdio")
    args = parser.parse_args()

    runner = PstatExport(*args.files)
    with sink_factory(parse(args.output)) as snk:
        snk.process(runner.iter_stats())


if __name__ == "__main__":
    main()
