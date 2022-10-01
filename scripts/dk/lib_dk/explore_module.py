# Copyright (c) 2017 Cobus Nel
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
Exploration sub system
"""
import collections
import importlib
import itertools
import sys
from re import RegexFlag

from . import module, options
from dkit.data import manipulate as mp, eda, iteration
from dkit.plot import ggrammar
from dkit.etl.extensions.ext_xlsx import XlsxSink
import logging

logger = logging.getLogger(__name__)


class ExploreModule(module.MultiCommandModule):

    def do_count(self):
        """count instances of specified field"""
        # hack to only extract the required field
        # The below disable extracting only specific
        # fields if a transform is specified. ideally the
        # program need to figure out from the transform object
        # which fields should be included.
        if self.args.transform is None:
            self.args.fields = [self.args.field]

        display_n = self.args.head if self.args.head > 0 else sys.maxsize - 1
        iter_input = self.input_stream_sampled(self.args.input)
        counter = collections.Counter(i[self.args.field] for i in iter_input)
        self.tabulate(
            {self.args.field: item, "count": v}
            for item, v in counter.most_common(display_n)
        )

    def do_distinct(self):
        """print distinct values for keys specified"""
        distinct_rows = mp.distinct(
            self.input_stream(self.args.input),
            *self.args.select_fields
        )

        if self.args.sort_output is True:
            distinct_rows = sorted(distinct_rows, reverse=self.args.reversed)

        if self.args.table:
            self.tabulate(distinct_rows)
        elif self.args.long is True:
            for row in distinct_rows:
                self.print(", ".join(row.values()))
        else:
            self.push_to_uri(
                self.args.output,
                distinct_rows
            )

    def do_duplicates(self):
        """print duplicate values for keys specified (each only once)"""
        duplicate_rows = mp.duplicates(
            self.input_stream(self.args.input),
            *self.args.select_fields
        )

        if self.args.sort_output is True:
            duplicate_rows = sorted(duplicate_rows, reverse=self.args.reversed)

        if self.args.table:
            self.tabulate(duplicate_rows)
        elif self.args.long is True:
            for row in duplicate_rows:
                self.print(", ".join(row.values()))
        else:
            self.push_to_uri(
                self.args.output,
                duplicate_rows
            )

    def do_fields(self):
        """print field names"""
        if self.args.table is True:
            from dkit.etl import schema
            cschema = schema.EntityValidator.dict_from_iterable(
                self.input_stream(self.args.input),  p=1, stop=100
            )
            dschema = [{"field": k, "type": v["type"]} for k, v in cschema.items()]
            self.tabulate(dschema)
        else:
            iter_input = itertools.islice(
                self.input_stream(self.args.input),
                100
            )
            headings = sorted(set(itertools.chain(*[list(k.keys()) for k in iter_input])))
            if self.args.long is True:
                for heading in headings:
                    self.print(heading)
            else:
                self.columnize(headings)

    def do_head(self):
        """print first n entries with optional sampling"""
        self.tabulate(
            self.input_stream(
                self.args.input
            )
        )

    def do_histogram(self):
        """generate histogram for field"""
        from dkit.data.stats import Accumulator
        from dkit.data.histogram import Histogram

        # hack to extract only required field
        self.args.fields = [self.args.field]
        field_name = self.args.field

        a = Accumulator((i[field_name] for i in self.input_stream(self.args.input)))
        h = Histogram.from_accumulator(a, precision=2)

        p = ggrammar.Plot(h) \
            + ggrammar.GeomHistogram(field_name, color="#FF0000", alpha=0.9) \
            + ggrammar.Title(f"Frequency distribution of {field_name}") \
            + ggrammar.YAxis("frequency") \
            + ggrammar.XAxis(field_name)

        self.__render_plot(p)

        if self.args.table is True:
            t = [b.as_dict() for b in h.bins]
            self.tabulate(t)

    def do_qhist(self):
        """generate quick histogram for specified field"""
        import plotille
        from dkit.shell import console
        width, _ = console.get_terminal_size()
        # hack to extract only required field
        self.args.fields = [self.args.field]
        field_name = self.args.field
        data = [i[field_name] for i in self.input_stream(self.args.input)]
        self.print(
            plotille.hist(
                data,
                width=75,
                bins=self.args.bins,
                lc="bright_green",
                log_scale=self.args.log
            )
        )

    def do_match(self):
        """regex match (match from start of text)"""
        from dkit.data import filters
        self.__do_regex(filters.match_filter)

    def __render_plot(self, grammar):
        """
        Render plot to terminal or to file depending on
        options set
        """
        from dkit.plot import gnuplot
        print(self.args.script)
        if self.args.output is not None:
            terminal = ggrammar.Plot.terminal_from_filename(self.args.output)
            gnuplot.BackendGnuPlot(terminal=terminal) \
                .render(grammar.as_dict(), self.args.output, self.args.script)
        else:
            self.print(
                gnuplot.BackendGnuPlot("dumb").render_str(grammar.as_dict())
            )

    def do_plot(self):
        """generate plot grammar"""
        from dkit.plot import ggrammar

        x_field = self.args.xfield
        y_field = self.args.yfield

        # hack to only extract the required field
        self.args.fields = [y_field]
        if x_field != "_index":
            self.args.fields.append(x_field)

        data = self.input_stream(self.args.input)
        geom = ggrammar.GEOM_MAP[self.args.plot_type]

        p = ggrammar.Plot(data) \
            + geom(y_field, y_data=y_field, x_data=x_field) \
            + ggrammar.XAxis(x_field) \
            + ggrammar.YAxis(y_field)

        if self.args.title is not None:
            p += ggrammar.Title(self.args.title)

        self.__render_plot(p)

    def __do_regex(self, re_filter):
        flags = 0
        if self.args.ignore_case is True:
            flags += RegexFlag.IGNORECASE

        fields = self.args.search_fields if self.args.search_fields else None

        # hack to only extract the required fields
        self.args.fields = fields

        is_match = re_filter(self.args.pattern, fields, flags)
        self.do_output(
            (row for row in self.input_stream(self.args.input) if is_match(row))
        )

    def do_search(self):
        """regex search (match anywhere in text)"""
        from dkit.data import filters
        self.__do_regex(filters.search_filter)

    def do_summary(self):
        """print summary for field"""
        from dkit.data.stats import Accumulator

        # hack to extract only required field
        self.args.fields = [self.args.field]

        a = Accumulator()
        field_name = self.args.field
        a.consume((a[field_name]) for a in self.input_stream(self.args.input))
        self.print(str(a))

    def do_strucmap(self):
        """create structure map of data"""
        r = eda.SchemaMap(800)
        data = list(self.input_stream(self.args.input))
        r.save_plot(self.args.output, data)

    def do_sample(self):
        """sample n records from each table in a database"""
        ext_sql_alchemy = importlib.import_module("dkit.etl.extensions.ext_sql_alchemy")
        services = self.load_services(ext_sql_alchemy.SQLServices)

        # get table names from glob
        all_table_names = services.get_sql_tables(self.args.connection)
        table_names = list(sorted(iteration.glob_list(all_table_names, self.args.table)))

        # extract sample
        sample = services.sample_from_db(
            self.args.connection,
            *table_names,
            n=self.args.n
        )

        # write to output
        logger.info(f"Writing sample output to '{self.args.output}'")
        XlsxSink(self.args.output).process_dict(sample)

    def do_table(self):
        """print all data in a table"""
        self.tabulate(
            self.input_stream(self.args.input)
        )

    def init_parser(self):
        """initialize argparse parser"""
        self.init_sub_parser("explore data")

        # count
        parser_count = self.sub_parser.add_parser("count", help=self.do_count.__doc__)
        options.add_option_defaults(parser_count)
        options.add_option_field_name(parser_count)
        options.add_options_sampling_input_all(parser_count)
        options.add_option_n(parser_count, -1)
        options.add_option_head(parser_count)

        # distinct
        parser_distinct = self.sub_parser.add_parser("distinct", help=self.do_distinct.__doc__)
        options.add_option_defaults(parser_distinct)
        options.add_options_minimal_inputs(parser_distinct)
        options.add_option_field_names(parser_distinct)
        options.add_option_sort_output(parser_distinct)
        options.add_option_reversed(parser_distinct)
        options.add_option_tabulate(parser_distinct)
        options.add_option_long_format(parser_distinct)
        options.add_option_output_uri(parser_distinct)

        # duplicates
        parser_duplicates = self.sub_parser.add_parser(
            "duplicates",
            help=self.do_duplicates.__doc__
        )
        options.add_option_defaults(parser_duplicates)
        options.add_options_minimal_inputs(parser_duplicates)
        options.add_option_field_names(parser_duplicates)
        options.add_option_sort_output(parser_duplicates)
        options.add_option_reversed(parser_duplicates)
        options.add_option_tabulate(parser_duplicates)
        options.add_option_long_format(parser_duplicates)
        options.add_option_output_uri(parser_duplicates)

        # fields
        parser_fields = self.sub_parser.add_parser("fields", help=self.do_fields.__doc__)
        options.add_option_defaults(parser_fields)
        options.add_options_minimal_inputs(parser_fields)
        options.add_option_long_format(parser_fields)
        options.add_option_tabulate(parser_fields)

        # search
        parser_search = self.sub_parser.add_parser("search", help=self.do_search.__doc__)
        options.add_option_regex(parser_search)
        options.add_option_tabulate(parser_search)

        # match
        parser_match = self.sub_parser.add_parser("match", help=self.do_match.__doc__)
        options.add_option_regex(parser_match)
        options.add_option_tabulate(parser_match)

        # head
        parser_head = self.sub_parser.add_parser("head", help=self.do_head.__doc__)
        options.add_option_defaults(parser_head)
        options.add_options_inputs(parser_head)
        options.add_option_n(parser_head, default=10)
        options.add_option_column_width(parser_head)
        options.add_option_transpose(parser_head)

        # histogram
        parser_histogram = self.sub_parser.add_parser("histogram", help=self.do_histogram.__doc__)
        options.add_option_defaults(parser_histogram)
        options.add_options_inputs(parser_histogram)
        options.add_option_field_name(parser_histogram)
        options.add_option_tabulate(parser_histogram)
        parser_histogram.add_argument("-o", "--output", help="output to file", default=None)
        parser_histogram.add_argument("--script", help="gnuplot script file (optional)",
                                      default=None)

        # qhist
        parser_q_histogram = self.sub_parser.add_parser(
            "qhist", help=self.do_qhist.__doc__
        )
        options.add_option_defaults(parser_q_histogram)
        options.add_options_inputs(parser_q_histogram)
        options.add_option_field_name(parser_q_histogram)
        parser_q_histogram.add_argument("--log", default=False, action="store_true",
                                        help="enable log scale")
        parser_q_histogram.add_argument("--bins", default=10, type=int,
                                        help="number of bins")

        # plot
        parser_plot = self.sub_parser.add_parser("plot", help=self.do_plot.__doc__)
        options.add_option_defaults(parser_plot)
        options.add_options_minimal_inputs(parser_plot)
        options.add_option_sort_fields(parser_plot)
        options.add_option_reversed(parser_plot)
        parser_plot.add_argument("-x", "--xfield", help="x axis field name",
                                 default="_index")
        parser_plot.add_argument("-y", "--yfield", help="y field name", required=True)
        parser_plot.add_argument("--type", dest="plot_type",
                                 help="plot type.",
                                 choices=list(ggrammar.GEOM_MAP.keys()), default="scatter")
        parser_plot.add_argument("--title", help="plot title", default=None)
        parser_plot.add_argument("-o", "--output", help="output to file", default=None)
        parser_plot.add_argument("--script", help="gnuplot script file (optional)",  default=None)

        # struc
        parser_struc = self.sub_parser.add_parser("struc", help=self.do_strucmap.__doc__)
        options.add_option_n(parser_struc, default=0)
        options.add_option_defaults(parser_struc)
        options.add_options_inputs(parser_struc)
        parser_struc.add_argument("-o", "--output", help="output to file", default="structure.pdf")

        # sample
        parser_sample = self.sub_parser.add_parser("sample", help=self.do_sample.__doc__)
        options.add_option_defaults(parser_sample)
        options.add_option_connection_name(parser_sample)
        options.add_option_n(parser_sample, default=1000)
        parser_sample.add_argument("-o", "--output", help="output to file (sample.xlsx)",
                                   default="sample.xlsx")
        options.add_option_glob(parser_sample, entity="table", help_="Table names")

        # table
        parser_table = self.sub_parser.add_parser("table", help=self.do_table.__doc__)
        options.add_option_n(parser_table, default=100)
        options.add_option_defaults(parser_table)
        options.add_options_inputs(parser_table)
        options.add_option_column_width(parser_table)
        options.add_option_transpose(parser_table)

        # summary
        parser_summary = self.sub_parser.add_parser("summary", help=self.do_summary.__doc__)
        options.add_option_defaults(parser_summary)
        options.add_options_inputs(parser_summary)
        options.add_option_field_name(parser_summary)

        super().parse_args()
