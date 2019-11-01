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
Maintain endpoints
"""

from . import module, options
from dkit.data import xml_helper as xh


class XMLModule(module.CRUDModule):

    def __init__(self, arguments):
        super().__init__(arguments, "xml")

    def do_add(self):
        """add xml parse rule"""
        pass

    def do_stats(self):
        """
        print statistics of xml file
        """
        with open(self.args.input[0]) as infile:
            xml_counter = xh.XMLStat(self.logger, self.log_trigger)(infile)

        # output sorting
        if self.args.sort_output is True:
            key = "count" if self.args.numeric else "path"
            tab_stats = sorted(
                [{"count": v, "path": k} for k, v in xml_counter.items()],
                key=lambda x: x[key],
                reverse=self.args.reversed
            )
        else:
            tab_stats = [{"count": v, "path": k} for k, v in xml_counter.items()]

        # output format
        if self.args.long:
            self.tabulate(tab_stats)
        else:
            for v in tab_stats:
                self.print(v["path"])

    def do_rm(self):
        """remove query from model"""
        # super().do_rm(self.args.)
        pass

    def do_print(self):
        """print query"""
        # model = self.load_services().model
        # container = getattr(model, self.entity_name)
        # item = container[self.args.query]
        # self.print(item.query)
        pass

    def init_parser(self):
        """initialize argparse parser"""
        self.init_sub_parser("maintain xml schemas")

        parser_ls = self.sub_parser.add_parser(
            "ls",
            help=self.do_ls.__doc__,
        )
        options.add_option_defaults(parser_ls)

        # stat
        parser_stat = self.sub_parser.add_parser(
            "stats",
            help=self.do_stats.__doc__
        )
        options.add_option_defaults(parser_stat)
        options.add_option_input_file(parser_stat)
        options.add_option_long_format(parser_stat)
        options.add_option_sort_output(parser_stat)
        options.add_option_numeric_sort(parser_stat)
        options.add_option_reversed(parser_stat)

        super().parse_args()
