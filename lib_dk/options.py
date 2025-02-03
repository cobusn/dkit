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

# import argparse

from dkit.utilities import cmd_helper
from dkit.etl import model
from . import defaults
import argparse
import sys


def add_arg_uri(parser, n="*"):
    """uri"""
    parser.add_argument(
        "uri",
        nargs=n,
        default=None,
        help=add_arg_uri.__doc__
    )


def add_options_minimal(parser):
    add_option_config(parser)
    add_option_model(parser)


def add_option_defaults(parser):
    add_option_logging(parser)
    add_option_config(parser)
    add_option_model(parser)


def add_option_alternate_model(parser):
    """alternate model"""
    parser.add_argument("--other", default=None,
                        help=add_option_alternate_model.__doc__)


def add_option_append(parser):
    """append to model"""
    parser.add_argument("--append", action="store_true", default=False,
                        help=add_option_append.__doc__)


def add_option_backend_map(parser):
    """backend for intermediate storage"""
    parser.add_argument("--backend", default=None, type=str,
                        help=add_option_backend_map.__doc__)


def add_option_config(parser):
    """add config file option"""
    parser.add_argument('--config', dest="config_uri", default="~/.dk.ini",
                        help="configuration file")


def add_option_column_width(parser):
    """maximum display column width"""
    parser.add_argument("-w", "--max-width", dest="width", default=0, type=int,
                        help=add_option_column_width.__doc__)


def add_option_connection_uri(parser):
    """connection uri"""
    parser.add_argument(
        dest="uri",
        help=add_option_connection_uri.__doc__
    )


def add_option_connection_name(parser):
    """connection name"""
    parser.add_argument("-c", "--connection", required=True, help=__doc__)


def add_option_secret_name(parser):
    """secret name"""
    parser.add_argument("-s", "--secret", required=True, help=__doc__)


def add_option_connection_name_opt(parser):
    """connection name"""
    parser.add_argument(
        "-c",
        "--connection",
        default=None,
        help=add_option_connection_name_opt.__doc__
    )


def add_option_cut(parser):
    """extract only named fields"""
    parser.add_argument(
        "--cut", action=cmd_helper.StoreList, dest="fields",
        help=add_option_cut.__doc__, metavar="FIELD1,FIELD2",
        default=None
    )


def add_option_description(parser):
    """description"""
    parser.add_argument("-d", "--description", type=str, default="")


def add_option_endpoint_uri(parser):
    """endpoint uri"""
    parser.add_argument(dest="uri", nargs=1, help=__doc__)


def add_option_endpoint_name(parser):
    """endpoint name"""
    parser.add_argument("-E", "--endpoint", required=True, help=__doc__)


def add_option_field_name(parser):
    """field name"""
    parser.add_argument("-d", "--field", help="field name", required=True)


def add_option_field_names(parser):
    """field names"""
    parser.add_argument("-d", "--field", dest="select_fields", action="append",
                        default=[], help="add field for aggregation")


def add_option_file(parser):
    """specify sql file"""
    parser.add_argument(
        '--file', dest="query_file",
        help=add_option_file.__doc__,
        default=None
    )


def add_option_filter(parser):
    """apply filter"""
    parser.add_argument(
       '-f', '--filter', dest="filter",
       help="apply filter",
    )


def add_option_pattern(parser):
    """regualar expression pattern"""
    parser.add_argument("pattern", default=".*",
                        help=add_option_pattern.__doc__)
    parser.add_argument("-I", "--ignore-case", default=False,
                        action="store_true", help="Ignore case")


def add_option_glob(parser, entity="glob", help_=None, default=None):
    """glob pattern(s)"""
    __help = help_ if help_ else add_option_glob.__doc__
    parser.add_argument(entity, nargs="*", default=["*"],
                        help=__help)


def add_option_head(parser):
    """display only n rows of output"""
    parser.add_argument("--head", default=-1, type=int,
                        help=add_option_head.__doc__)


def add_option_input_uri_optional(parser):
    """input uri e.g. (mysql://user:passw@host:port/db)"""
    parser.add_argument(dest="input", nargs="?",
                        help=add_option_input_uri_optional.__doc__)


def add_option_input_db_uri_optional(parser):
    """database URI"""
    parser.add_argument(
        dest="database_uri", nargs="?",
        help=add_option_input_uri_optional.__doc__
    )


def add_option_input_file(parser):
    """input files"""
    parser.add_argument(dest="input", nargs=1, help='input file')


def add_option_input_uris(parser):
    """input files"""
    parser.add_argument(dest="input", nargs="+", help='input uri(s)')


def add_option_long_format(parser):
    parser.add_argument("-l", "--long", action="store_true", default=False,
                        help="display in long format")


def add_option_n(parser, default=defaults.DEFAULT_N):
    """read first n records of input (default={})"""
    parser.add_argument("-n", dest="n", type=int, default=default,
                        help=add_option_n.__doc__.format(default))


def add_option_numeric_sort(parser, default=defaults.DEFAULT_N):
    """sort output as numeric values"""
    parser.add_argument("-N", "--numeric", default=False, action="store_true",
                        help=add_option_numeric_sort.__doc__.format(default))


def add_option_model(parser):
    parser.add_argument('-m', '--model', dest="model_uri",
                        help="uri of  model", default=model.DEFAULT_MODEL_FILE)


def add_option_model_required(parser):
    parser.add_argument('-m', '--model', dest="model_uri",
                        help="uri of  model", required=True)


def add_option_output_uri(parser):
    """output option"""
    parser.add_argument('-o', dest="output", help='output filename. default to jsonl on stdout',
                        default="jsonl:///stdio")


def add_option_output(parser):
    """output options"""
    parser.add_argument(
        '-o', dest="output", type=argparse.FileType('wt'),
        help='output filename to stdout', default=sys.stdout
    )


def add_option_enter_password(parser):
    """force interactive password entry"""
    parser.add_argument("-P", "--password", dest="enter_password",
                        default=None, action="store_true",
                        help=add_option_enter_password.__doc__)


def add_option_query(parser):
    """stored query"""
    parser.add_argument(
        "-q", dest="query", help=add_option_query.__doc__,
        default=None
    )


def add_option_query_string(parser):
    """query string supplied on command line"""
    parser.add_argument(
        "--query", dest="query_string",
        help=add_option_query_string.__doc__,
        default=None
    )


def add_option_relation_name(parser):
    """endpoint name"""
    parser.add_argument("-r", "--relation", default=None,
                        help=add_option_relation_name.__doc__)


def add_options_diff_fields(parser):
    parser.add_argument("--value", dest="values", action="append", default=[],
                        help="add value field (can be added multiple times)")


def add_options_diff(parser):
    # input options
    add_options_extension(parser)
    add_option_entity_optional(parser)
    add_option_transform_name(parser)
    add_option_filter(parser)

    parser.add_argument("-a", required=True, help="uri for dataset a")
    parser.add_argument("-b", required=True, help="uri for dataset b")
    parser.add_argument("-k", "--key", dest="keys", action="append",
                        default=[], help="add key field (can be added multiple times)")
    parser.add_argument("--huge", default=False, action="store_true",
                        help="process files too big for memory")


def add_options_join(parser):
    """add relation options"""
    parser.add_argument("-L", "--left", required=True,
                        help="uri for constrained entity")
    parser.add_argument("-R", "--right", required=True,
                        help="uri for referred entity")
    parser.add_argument("-l", "--lc", action='append', dest="const_cols", required=True,
                        help="append constrained column")
    parser.add_argument("-r", "--rc", action='append', dest="ref_cols", required=True,
                        help="append referred column")
    parser.add_argument("--all.left", action="store_true", dest="all_left", default=False,
                        help="include all left rows")


def add_option_relation_add(parser):
    """add relation options"""
    parser.add_argument("-M", "--many", required=True,
                        help="name of constrained entity")
    parser.add_argument("-O", "--one", required=True,
                        help="name of referred entity")
    parser.add_argument("--mc", action='append', dest="const_cols", required=True,
                        help="append constrained column")
    parser.add_argument("--oc", action='append', dest="ref_cols", required=True,
                        help="append referred column")


def add_option_regex(parser):
    add_option_config(parser)
    add_option_model(parser)
    add_options_extension(parser)
    add_option_cut(parser)
    add_option_n(parser)
    add_option_output_uri(parser)
    parser.add_argument("-e", "--pattern", help="search pattern", required=True)
    parser.add_argument(
        "-d", "--field", dest="search_fields", action="append",
        default=[], help="fields to search, add repeatedly for more than one"
    )
    parser.add_argument(
        "-i", "--ignore-case", dest="ignore_case", action="store_true",
        default=False, help="ignore case"
    )
    add_option_input_uris(parser)


def add_query_group(parser):
    g = parser.add_mutually_exclusive_group(required=True)
    add_option_query(g)
    add_option_file(g)
    add_option_query_string(g)


def add_option_reversed(parser):
    """reverse sort order"""
    parser.add_argument("--reversed", action="store_true", default=False,
                        help=add_option_reversed.__doc__)


def add_option_entity(parser):
    """add option to name schema"""
    parser.add_argument('-e', '--entity', help="entity name", required=True)


def add_option_entity_or_yaml(parser):
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-e', '--entity', help="entity name")
    group.add_argument(
        '--yaml', help="entity from yaml instead of model"
    )


def add_arg_entities(parser):
    """ltst of entities"""
    parser.add_argument("entities", metavar="entity", nargs="+",
                        default=[],
                        type=str, help=add_arg_entities.__doc__)


def add_option_entity_optional(parser):
    """add option to name schema"""
    parser.add_argument('-e', '--entity', help="entity name", default=None)


def add_option_sort_fields(parser):
    """sort using these keys"""
    parser.add_argument('--sort', help=add_option_sort_fields.__doc__,
                        action=cmd_helper.StoreList, metavar="FIELD1,FIELD2,..",
                        default=None)


def add_option_sort_output(parser):
    """sort output"""
    parser.add_argument('--sort', dest="sort_output", help=add_option_sort_output.__doc__,
                        action="store_true", default=False)


def add_option_table_name(parser):
    """add option for table name"""
    parser.add_argument('-b', "--table", dest="table", help="table name")


def add_option_template(parser):
    """template file name"""
    parser.add_argument(
        "--template", dest="template", required=True,
        help=add_option_template.__doc__
    )


def add_option_transform_name(parser):
    """add option for transform name"""
    parser.add_argument('-t', dest="transform", help="transform name")


def add_option_tabulate(parser):
    """display in table format (limited to 100 rows)"""
    parser.add_argument('--table', action="store_true", default=False,
                        help=add_option_tabulate.__doc__)


def add_option_logging(parser):
    """add option for transform name"""
    me_group = parser.add_mutually_exclusive_group()
    me_group.add_argument('-v', '--verbose', dest="verbose", default=False, action="store_true",
                          help="display informational messages")
    me_group.add_argument('--warning', default=False, action="store_true",
                          help="display warning messages")
    me_group.add_argument('--debug', default=False, action="store_true",
                          help="display debug messages")
    parser.add_argument('--trigger', dest="log_trigger", default=None, type=int,
                        help="Trigger log action on this number of records")


def add_option_transpose(parser):
    """transpose rows and columns"""
    parser.add_argument("--transpose", dest="transpose", action="store_true", default=False,
                        help=add_option_transpose.__doc__)


def add_option_query_name(parser):
    """query name"""
    parser.add_argument("-q", "--query", dest="query", required=True, help=__doc__)


def add_option_kw_data(parser):
    """input uri's as key value pair"""
    parser.add_argument(
        '--data', dest="data_dict", action="append",
        metavar="KEY1=URI1,KEY2=URI2", help=add_option_kw_data.__doc__,
        default=[]
    )


def add_option_yes(parser):
    """always answer yes"""
    parser.add_argument('-y', "--yes", action="store_true", default=False,
                        help="always answer yes")


def add_options_sampling(parser, n=defaults.DEFAULT_SAMPLE_SIZE):
    """options for sampling"""
    parser.add_argument(
        '-p', '--sample-probability', dest="sample_probability",
        type=float,
        help="probability (default={})".format(defaults.DEFAULT_PROBABILITY),
        default=defaults.DEFAULT_PROBABILITY,
    )
    parser.add_argument(
        '-k', '--size', dest="sample_size",
        help="sample_size (0=all, default={})".format(n),
        default=n,
        type=int
    )


def add_options_sampling_input_all(parser, k=0):
    """options for input enpoint"""
    add_options_sampling(parser, k)
    add_options_inputs(parser)


def add_options_sampling_input(parser, k=defaults.DEFAULT_SAMPLE_SIZE):
    """options for input enpoint"""
    add_options_sampling(parser, k)
    add_options_inputs(parser)


def add_options_extension(parser):
    """Add CSV Options to parsser"""
    parser.add_argument('--skip', dest="skip_lines",
                        help="[CSV only] skip number of lines in input file",
                        default=0, type=int)
    parser.add_argument('--headings', dest="headings",
                        help="[CSV only] file that define headings (one heading per line)",
                        default=None)
    parser.add_argument('--delimiter', dest="delimiter",
                        help="[CSV only] field delimiter",
                        default=",")
    parser.add_argument('--sheet', dest="work_sheet",
                        help="[XLSX only] worksheet name",
                        default=None)


def add_option_where(parser):
    """native where clause (e.g sql or pytables)"""
    parser.add_argument("--where", help=add_option_where.__doc__, default=None)


def add_options_raw_input(parser):
    """
    add all input option
    """
    add_option_model(parser)
    add_options_extension(parser)


def add_options_minimal_inputs(parser):
    """minimal input options"""
    add_options_extension(parser)
    add_option_entity_optional(parser)
    add_option_transform_name(parser)
    add_option_filter(parser)
    add_option_input_uris(parser)


def add_options_inputs(parser):
    """
    add all input option
    """
    # add_options_input_type(parser)
    add_option_entity_optional(parser)
    add_option_transform_name(parser)
    add_option_filter(parser)
    add_options_extension(parser)
    add_option_cut(parser)
    add_option_sort_fields(parser)
    add_option_reversed(parser)
    add_option_input_uris(parser)
    add_option_where(parser)
