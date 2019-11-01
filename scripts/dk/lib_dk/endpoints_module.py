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
from dkit.etl import model
from dkit.etl.extensions import ext_sql_alchemy


class EndpointsModule(module.CRUDModule):

    def __init__(self, arguments):
        super().__init__(arguments, "endpoints")

    def do_add(self):
        """
        create endpoint (model with schema)
        """
        services = self.load_services()
        m = services.model
        container = getattr(m, self.entity_name)
        if self.args.endpoint in container:
            raise exceptions.CkitApplicationException(
                "endpoint '{}' exists already".format(self.args.endpoint)
            )
        else:
            endpoint_name = self.args.endpoint
            conn_name = self.args.connection
            entity_name = self.args.entity
            table_name = self.args.table
            container[endpoint_name] = model.Endpoint(
                connection=conn_name,
                entity=entity_name,
                table_name=table_name,
            )
            services.save_model_file(m, self.args.model_uri)

    def do_rm(self):
        """remove endpoint"""
        super().do_rm(self.args.endpoint)

    def do_print(self):
        """print endpoint details"""
        super().do_print(self.args.endpoint)

    def do_create_h5_table(self):
        """
        create hdf5 table from endpoint
        """
        from dkit.etl.extensions import ext_tables as h5
        h5_services = h5.PyTablesServices.from_file(
            self.args.model_uri,
            self.args.config_uri
        )
        endpoint_name = self.args.endpoint
        h5_services.create_table(endpoint_name)

    def do_create_sql(self):
        """
        create sql table from endpoint
        """
        services = self.load_services(ext_sql_alchemy.SQLServices)
        endpoint = self.args.endpoint
        services.create_sql_table(endpoint)

    def init_parser(self):
        """initialize argparse parser"""
        self.init_sub_parser("maintain and run transforms")

        # ls
        parser_ls = self.sub_parser.add_parser(
            "ls",
            help=self.do_ls.__doc__,
        )
        options.add_option_defaults(parser_ls)

        # add
        parser_add = self.sub_parser.add_parser(
            "add",
            help="add endpoint"
        )
        options.add_option_config(parser_add)
        options.add_option_model(parser_add)
        options.add_option_endpoint_name(parser_add)
        options.add_option_connection_name(parser_add)
        options.add_option_entity(parser_add)
        options.add_option_table_name(parser_add)

        # print
        parser_print = self.sub_parser.add_parser(
            "print",
            help=self.do_print.__doc__,
        )
        options.add_option_defaults(parser_print)
        options.add_option_endpoint_name(parser_print)

        # rm
        parser_rm = self.sub_parser.add_parser(
            "rm",
            help=self.do_rm.__doc__
        )
        options.add_option_config(parser_rm)
        options.add_option_model(parser_rm)
        options.add_option_endpoint_name(parser_rm)

        # create.sql
        parser_create_sql = self.sub_parser.add_parser(
            "create_sql",
            help=self.do_create_sql.__doc__
        )
        options.add_option_config(parser_create_sql)
        options.add_option_model(parser_create_sql)
        options.add_option_endpoint_name(parser_create_sql)

        # create_h5
        parser_create_h5 = self.sub_parser.add_parser(
            "create_h5",
            help=self.do_create_h5_table.__doc__
        )
        options.add_option_config(parser_create_h5)
        options.add_option_model(parser_create_h5)
        options.add_option_endpoint_name(parser_create_h5)

        super().parse_args()
