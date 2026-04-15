# Copyright (c) 2026 Cobus Nel
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
CLI tool for inspecting and editing JSON and YAML configuration files.

Supports an interactive shell (edit), non-interactive reads (get, tree),
and non-interactive writes (set).
"""
from config_shell import ConfigShell, coerce_value
import argparse
import dotenv
import configparser
import os

DEFAULT_TOKEN_KEY = "key"


def get_token(env_variable, init_file):
    if env_variable is not None:
        dotenv.load_dotenv()
        return os.environ.get(env_variable)
    elif init_file is not None:
        parser = configparser.ConfigParser()
        parser.read(os.path.expanduser(init_file))
        return parser["DEFAULT"].get(DEFAULT_TOKEN_KEY, None)
    else:
        return None


def add_file_options(parser):
    parser.add_argument("file", help="json/yaml file to edit")


def add_path_option(parser):
    parser.add_argument("path", help="path to option (e.g. operational/server)")


def parse_args():
    parser = argparse.ArgumentParser(prog='cfgctl', description=__doc__)

    token_group = parser.add_mutually_exclusive_group()
    token_group.add_argument('-i', '--init-file', help='init file', default=None)
    token_group.add_argument('-e', '--env-variable', default=None,
                             help="name of environmental variable for token")

    sub_parsers = parser.add_subparsers(
        help='sub-command help',
        dest="command",
        required=True
    )

    # edit
    edit_parser = sub_parsers.add_parser("edit")
    add_file_options(edit_parser)

    # tree
    tree_parser = sub_parsers.add_parser("tree", help="display config as tree")
    add_file_options(tree_parser)

    # get
    get_parser = sub_parsers.add_parser("get", help="get value")
    add_file_options(get_parser)
    add_path_option(get_parser)

    # set
    set_parser = sub_parsers.add_parser("set", help="set value")
    add_file_options(set_parser)
    add_path_option(set_parser)
    set_parser.add_argument("value", help="configuration value")

    return parser.parse_args()


def dispatch(args):
    shell = ConfigShell(
        args.file,
        get_token(args.env_variable, args.init_file)
    )
    match args.command:
        case "edit":
            shell.cmdloop()
        case "tree":
            shell.do_tree("")
        case "get":
            shell.do_get(args.path)
        case "set":
            shell._set_value(args.path, coerce_value(args.value))
            shell.do_save("")


def main():
    try:
        dispatch(parse_args())
    except Exception as E:
        print(E)


if __name__ == "__main__":
    main()
