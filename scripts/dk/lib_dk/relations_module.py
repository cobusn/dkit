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
Maintain relations
"""

from . import module, options
from dkit.utilities import iter_helper
from dkit.etl.extensions import ext_sql_alchemy


class RelationsModule(module.CRUDModule):
    """
    Maintain entity relationships
    """
    def __init__(self, arguments):
        super().__init__(arguments, "relations")

    def do_add(self):
        """
        create relation between entities
        """
        if self.args.relation is None:
            relation_name = "{}_{}".format(
                self.args.constrained_entity.lower(),
                self.args.referred_entity.lower()
            )
        else:
            relation_name = self.args.relation

        services = self.load_services(ext_sql_alchemy.SQLServices)

        services.model.add_relation(
            relation_name,
            self.args.constrained_entity,
            self.args.referred_entity,
            self.args.const_cols,
            self.args.ref_cols
        )

        services.save_model_file(services.model, self.args.model_uri)

    def do_rm(self):
        """remove relation"""
        super().do_rm(self.args.relation)

    def do_print(self):
        """print relation details"""
        super().do_print(self.args.relation)

    def do_sql_reflect(self):
        """reflect entities in sql database"""
        services = self.load_services(ext_sql_alchemy.SQLServices)
        table_names = services.get_sql_tables(self.args.connection)
        reflect_names = list(sorted(iter_helper.glob_list(table_names, self.args.glob)))
        for table_name in reflect_names:
            self.print(f"\nReflecting relations for table '{table_name}':")
            e = services.get_sql_table_relations(
                self.args.connection,
                table_name,
                self.args.append
            )
            if len(e) > 0:
                self.tabulate([{
                    "name": k,
                    "constrained": v.constrained_entity,
                    "referred": v.referred_entity
                } for k, v in e.items()])
            else:
                self.print("+ No Relations found")
        services.save_model_file(
            services.model,
            self.args.model_uri
        )

    def init_parser(self):
        """initialize argparse parser"""
        self.init_sub_parser("maintain and run relations")

        # add
        parser_add = self.sub_parser.add_parser(
            "add",
            help="add relation"
        )
        options.add_option_defaults(parser_add)
        options.add_option_relation_name(parser_add)
        options.add_option_relation_add(parser_add)

        # ls
        parser_ls = self.sub_parser.add_parser(
            "ls",
            help=self.do_ls.__doc__,
        )
        options.add_option_defaults(parser_ls)
        options.add_option_long_format(parser_ls)

        # print
        parser_print = self.sub_parser.add_parser(
            "print",
            help=self.do_print.__doc__,
        )
        options.add_option_defaults(parser_print)
        options.add_option_relation_name(parser_print)

        # rm
        parser_rm = self.sub_parser.add_parser(
            "rm",
            help=self.do_rm.__doc__
        )
        options.add_option_defaults(parser_rm)
        options.add_option_relation_name(parser_rm)

        # sql-reflect
        parser_sql_reflect = self.sub_parser.add_parser("sql-reflect",
                                                        help=self.do_sql_reflect.__doc__)
        options.add_option_defaults(parser_sql_reflect)
        options.add_option_connection_name(parser_sql_reflect)
        options.add_option_append(parser_sql_reflect)
        options.add_option_glob(parser_sql_reflect)
        super().parse_args()

        super().parse_args()
