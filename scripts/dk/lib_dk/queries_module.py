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
from dkit import exceptions


class QueriesModule(module.CRUDModule):

    def __init__(self, arguments):
        super().__init__(arguments, "queries")

    def do_add(self):
        """
        add query from file
        """
        services = self.load_services()
        model = services.model
        container = getattr(model, self.entity_name)
        if self.args.query in container:
            raise exceptions.CkitApplicationException(
                "query '{}' exists already".format(self.args.query)
            )
        else:
            with open(self.args.query_file, "r") as infile:
                obj_type = model.schema[self.entity_name]
                instance = obj_type(
                    query=infile.read(),
                    description=self.args.description
                )
                container[self.args.query] = instance
            services.save_model_file(
                services.model,
                self.args.model_uri
            )

    def do_rm(self):
        """remove query from model"""
        super().do_rm(self.args.query)

    def do_print(self):
        """print query"""
        model = self.load_services().model
        container = getattr(model, self.entity_name)
        item = container[self.args.query]
        self.print(item.query)

    def init_parser(self):
        """initialize argparse parser"""
        self.init_sub_parser("maintain and run transforms")

        parser_ls = self.sub_parser.add_parser(
            "ls",
            help=self.do_ls.__doc__,
        )
        options.add_option_defaults(parser_ls)

        # add
        parser_add = self.sub_parser.add_parser(
            "add",
            help=self.do_add.__doc__,
        )
        options.add_option_defaults(parser_add)
        options.add_option_query_name(parser_add)
        options.add_option_description(parser_add)
        options.add_option_file(parser_add)

        # print
        parser_print = self.sub_parser.add_parser(
            "print",
            help=self.do_print.__doc__,
        )
        options.add_option_defaults(parser_print)
        options.add_option_query_name(parser_print)

        # rm
        parser_rm = self.sub_parser.add_parser(
            "rm",
            help=self.do_rm.__doc__
        )
        options.add_option_defaults(parser_rm)
        options.add_option_query_name(parser_rm)

        super().parse_args()
