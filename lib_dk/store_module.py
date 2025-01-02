# Copyright (c) 2024 Cobus Nel
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
Maintain secrets
"""

from . import module, options
import json
import pyperclip


class VaultModule(module.CRUDModule):

    def __init__(self, arguments):
        super().__init__(arguments, "secrets")

    def do_add(self):
        """add a secret"""
        services = self.load_services()
        key = self.get_password("Key: ")
        secret = self.get_password("Secret: ")
        params = self.args.file if self.args.file else self.args.params

        services.model.add_secret(
            self.args.secret,
            key=key,
            secret=secret,
            parameters=params
        )
        services.save_model_file(
            services.model,
            self.args.model_uri
        )

    def do_help(self):
        self.print(self.__doc__)

    def do_rm(self):
        """remove connection"""
        super().do_rm(self.args.secret)

    def do_show(self):
        """print secret"""
        services = self.load_services()
        secret = services.model.get_secret(self.args.secret)
        self.print(json.dumps(secret.as_dict(), indent=4))

    def do_copy(self):
        """copy secret element to clipboard"""
        services = self.load_services()
        secret = services.model.get_secret(self.args.secret)
        match self.args.what:

            case "key":
                pyperclip.copy(secret.key)

            case "secret":
                pyperclip.copy(secret.secret)

            case "params":
                pyperclip.copy(json.dumps(secret.parameters))

            case _:
                pyperclip.copy(json.dumps(secret.as_dict()))

    def load_json(self, raw_text):
        """load json parameters from raw text"""
        data = json.loads(raw_text)
        if not isinstance(data, dict):
            raise TypeError("raw text should be json encoded dict")
        return data

    def load_jsonf(self, filename):
        """load json parameters from file"""
        with open(filename, "rt") as infile:
            data = json.load(infile)
        if not isinstance(data, dict):
            raise TypeError("raw text should be json encoded dict")
        return data

    def init_parser(self):
        """initialize argparse parser"""
        self.init_sub_parser("maintain secrets")

        parser_ls = self.sub_parser.add_parser(
            "ls",
            help=self.do_ls.__doc__,
        )
        options.add_option_defaults(parser_ls)

        # add
        parser_add = self.sub_parser.add_parser(
            "add",
            help="add secret"
        )
        options.add_option_defaults(parser_add)
        options.add_option_secret_name(parser_add)
        pgroup = parser_add.add_mutually_exclusive_group()
        pgroup.add_argument(
            "--params", help="json dict parameters", type=self.load_json,
            default=None
        )
        pgroup.add_argument(
            "--file", help="read params from json file", type=self.load_jsonf,
            default=None
        )

        # rm
        parser_rm = self.sub_parser.add_parser(
            "rm",
            help=self.do_rm.__doc__
        )
        options.add_option_defaults(parser_rm)
        options.add_option_secret_name(parser_rm)

        # copy
        parser_copy = self.sub_parser.add_parser(
            "copy",
            help=self.do_copy.__doc__
        )
        options.add_option_defaults(parser_copy)
        options.add_option_secret_name(parser_copy)
        parser_copy.add_argument(
            "what", choices=["key", "secret", "params", "json"],
        )

        # show
        parser_show = self.sub_parser.add_parser(
            "show",
            help=self.do_show.__doc__
        )
        options.add_option_defaults(parser_show)
        options.add_option_secret_name(parser_show)

        # help
        self.sub_parser.add_parser(
            "help",
            help=self.do_copy.__doc__
        )

        super().parse_args()
