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
Maintain connections
"""

from . import module, options
from textwrap import dedent


class ConnectionsModule(module.CRUDModule):

    def __init__(self, arguments):
        super().__init__(arguments, "connections")

    def do_add(self):
        """
        create transform from entity entity
        """
        services = self.load_services()
        if self.args.enter_password:        # pragma: no cover
            password = self.get_password()
        else:
            password = None

        services.model.add_connection(
            self.args.connection,
            self.args.uri,
            password
        )
        services.save_model_file(
            services.model,
            self.args.model_uri
        )

    def do_help(self):
        """print help on connections"""
        h = """
        azure
        require login to azure account (az login)
          mssql+pyodbc://@my-server.database.windows.net/myDb?driver=ODBC+Driver+17+for+SQL+Server

        mysql
          mysql+mysqldb://user:now&zzy@sample-db.co.za:3306/database

        athena:
          awsathena+rest://@athena.af-south-1.amazonaws.com:443/db?s3_staging_dir=athena.af-south-1.amazonaws.com

        oracle:
          oracle+cx_oracle://user:pass@host:1521/PROD

        postgres:
            postgresql://user:pass@host/5432

        sqlite:
            sqlite://filename.db
        """
        self.print(dedent(h))

    def do_rm(self):
        """remove connection"""
        super().do_rm(self.args.connection)

    def do_print(self):
        """print entity details"""
        super().do_print(self.args.connection)

    def init_parser(self):
        """initialize argparse parser"""
        self.init_sub_parser("maintain connections")

        parser_ls = self.sub_parser.add_parser(
            "ls",
            help=self.do_ls.__doc__,
        )
        options.add_option_defaults(parser_ls)

        # add
        parser_add = self.sub_parser.add_parser(
            "add",
            help="add connection"
        )
        options.add_option_defaults(parser_add)
        options.add_option_connection_name(parser_add)
        options.add_option_enter_password(parser_add)
        options.add_option_connection_uri(parser_add)

        # print
        parser_print = self.sub_parser.add_parser(
            "print",
            help=self.do_print.__doc__,
        )
        options.add_option_defaults(parser_print)
        options.add_option_connection_name(parser_print)

        # rm
        parser_rm = self.sub_parser.add_parser(
            "rm",
            help=self.do_rm.__doc__
        )
        options.add_option_defaults(parser_rm)
        options.add_option_connection_name(parser_rm)

        # help
        self.sub_parser.add_parser(
            "help",
            help=self.do_help.__doc__
        )

        super().parse_args()
