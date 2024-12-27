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
Find differences between two datasets
"""
from itertools import islice
from . import module, options
from dkit.data.diff import Compare

class DiffModule(module.MultiCommandModule):

    @property
    def _keys(self):
        k = self.args.keys if len(self.args.keys) > 0 else None
        return k

    def __output(self, result):
        if self.args.table:
            self.tabulate(list(islice(result, 1000)))
        else:
            self.push_to_uri(
                self.args.output,
                result
            )

    def do_added(self):
        """records in B but not in A"""
        a = self.input_stream([self.args.a])
        b = self.input_stream([self.args.b])
        c = Compare(a, b, keys=self._keys, huge=self.args.huge)
        result = c.added()
        self.__output(result)

    def do_deleted(self):
        """records in A but not in B"""
        a = self.input_stream([self.args.a])
        b = self.input_stream([self.args.b])
        c = Compare(a, b, keys=self._keys, huge=self.args.huge)
        result = c.deleted()
        self.__output(result)

    def do_changed(self):
        """records with changed values in specified fields"""
        a = self.input_stream([self.args.a])
        b = self.input_stream([self.args.b])
        c = Compare(a, b, keys=self._keys, huge=self.args.huge)

        result = c.changed(*self.args.values)
        self.__output(result)

    def do_deltas(self):
        """deltas for records with changed numerical values in specified fields"""
        a = self.input_stream([self.args.a])
        b = self.input_stream([self.args.b])
        c = Compare(a, b, keys=self._keys, huge=self.args.huge)

        result = c.deltas(*self.args.values)
        self.__output(result)

    def init_parser(self):
        self.init_sub_parser()

        # added
        parser_added = self.sub_parser.add_parser("added", help=self.do_added.__doc__)
        options.add_option_defaults(parser_added)
        options.add_options_diff(parser_added)
        options.add_option_output_uri(parser_added)
        options.add_option_tabulate(parser_added)

        # changed
        parser_changed = self.sub_parser.add_parser("changed", help=self.do_changed.__doc__)
        options.add_option_defaults(parser_changed)
        options.add_options_diff(parser_changed)
        options.add_options_diff_fields(parser_changed)
        options.add_option_output_uri(parser_changed)
        options.add_option_tabulate(parser_changed)

        # deleted
        parser_deleted = self.sub_parser.add_parser("deleted", help=self.do_deleted.__doc__)
        options.add_option_defaults(parser_deleted)
        options.add_options_diff(parser_deleted)
        options.add_option_output_uri(parser_deleted)
        options.add_option_tabulate(parser_deleted)

        # deltas
        parser_deltas = self.sub_parser.add_parser("deltas", help=self.do_deltas.__doc__)
        options.add_option_defaults(parser_deltas)
        options.add_options_diff(parser_deltas)
        options.add_options_diff_fields(parser_deltas)
        options.add_option_output_uri(parser_deltas)
        options.add_option_tabulate(parser_deltas)
        super().parse_args()

