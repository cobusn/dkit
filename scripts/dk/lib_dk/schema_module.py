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
schema subprocessing system
"""
import importlib
import re
import sys

from . import module, options
from dkit.etl import model
from dkit.etl.extensions import ext_sql_alchemy as sa
from dkit.parsers import uri_parser
from dkit.data import iteration


class SchemaModule(module.CRUDModule):

    def __init__(self, arguments):
        super().__init__(arguments, "entities")

    def do_grep(self):
        """
        regular expression search of entity names
        """
        services = self.load_services()
        flags = re.IGNORECASE if self.args.ignore_case else 0
        results = list(services.model.grep_entity_names(self.args.pattern, flags))
        self.columnize(results)

    def do_fgrep(self):
        """
        regular expression search of field names
        """
        services = self.load_services()
        flags = re.IGNORECASE if self.args.ignore_case else 0
        results = [
            {"entity": i[0], "field": i[1]}
            for i in
            services.model.grep_fields(self.args.pattern, flags)
        ]
        self.tabulate(results)

    def do_infer(self):
        """
        infer model from input file and save to specified schema

        print yaml of schema
        """
        ent = model.Entity.from_iterable(
             self.input_stream_raw(self.args.input),
             self.args.sample_probability,
             self.args.sample_size
        )
        self.print(str(ent))
        if self.args.entity is not None:
            services = model.ETLServices.from_file(
                self.args.model_uri,
                self.args.config_uri
            )
            services.model.entities[self.args.entity] = ent
            services.save_model_file(
                services.model,
                self.args.model_uri
            )

    def do_print(self):
        """print a schema"""
        super().do_print(self.args.entity)

    def do_rm(self):
        """remove a schema"""
        super().do_rm(self.args.entity)

    def do_export(self):
        """export schema to specified format"""
        do_close = False    # used to close file, if opened
        services = self.load_services()
        all_entity_names = list(services.model.entities.keys())
        entity_names = list(
            sorted(
                iteration.glob_list(all_entity_names, self.args.glob)
            )
        )

        # export model
        if self.args.type == "model":
            services.export_model_entities(entity_names, self.args.output)
        else:
            # set any additional options
            opts = {}
            if self.args.type in ["sql.create", "sql.select"]:
                opts["dialect"] = self.args.dialect

            # open file handle
            if self.args.output is None:
                outfile = sys.stdout
            else:
                do_close = True
                outfile = open(self.args.output, "w")

            # perform export
            services.export_schema(
                entity_names,
                self.args.type,
                outfile,
                opts
            )

            # cleanup
            if do_close:
                outfile.close()

    def do_h5_ls(self):
        """list tables in hdf5 database"""
        ext_h5 = importlib.import_module("dkit.etl.extensions.ext_tables")
        services = self.load_services(ext_h5.PyTablesServices)
        if self.args.uri is not None:
            db_name = self.args.uri
        else:
            conn = self.services.model.connections[self.args.connection]
            db_name = conn.database
        table_names = services.get_table_names(db_name)
        if self.args.long:
            self.tabulate(list(table_names))
        else:
            for row in table_names:
                self.print(row["path"])

    def do_sql_ls(self):
        """list tables in a sql database"""
        ext_sql_alchemy = importlib.import_module("dkit.etl.extensions.ext_sql_alchemy")
        services = self.load_services(ext_sql_alchemy.SQLServices)
        table_names = services.get_sql_tables(self.args.connection)
        self.columnize(table_names)

    def do_h5_reflect(self):
        """reflect entity in a HDF5 database"""
        ext_h5 = importlib.import_module("dkit.etl.extensions.ext_tables")
        services = self.load_services(ext_h5.PyTablesServices)
        if self.args.connection is not None:
            conn = services.model.connections[self.args.connection]
            file_name = conn.database
            full_path = self.args.uri[0]
        else:
            uri = uri_parser.parse(self.args.uri[0])
            file_name = uri["database"]
            full_path = uri["entity"]

        entity = services.do_h5_reflect(file_name, full_path)

        if self.args.entity is not None:
            model = services.model
            model.entities[self.args.entity] = entity
            services.save_model_file(
                model,
                self.args.model_uri
            )
        self.print(entity)

    def do_sql_reflect(self):
        """reflect entities in sql database"""
        ext_sql_alchemy = importlib.import_module("dkit.etl.extensions.ext_sql_alchemy")
        services = self.load_services(ext_sql_alchemy.SQLServices)

        table_names = services.get_sql_tables(self.args.connection)
        reflect_names = list(sorted(iteration.glob_list(table_names, self.args.glob)))
        for table_name in reflect_names:
            self.print("\n" + table_name)
            e = services.get_sql_table_schema(
                self.args.connection, table_name,
                self.args.append
            )
            self.tabulate([{"name": k, "type": v} for k, v in e.as_dict().items()])
        services.save_model_file(services.model, self.args.model_uri)

    def do_import(self):
        """import external entities"""
        services = self.load_services()
        other = services.load_alternate_model(self.args.other)
        for entity in self.args.entities:
            self.print(f"importing {entity}")
            e = other.entities[entity]
            services.model.entities[entity] = e
            services.save_model_file(services.model, self.args.model_uri)

    def do_show_types(self):
        """print table of cannonical data types"""
        from dkit.etl.extensions.ext_arrow import ARROW_TYPEMAP as arrow
        from dkit.etl.extensions.ext_avro import AVRO_TYPEMAP
        from dkit.etl.extensions.ext_spark import SchemaGenerator
        from dkit.etl.schema import EntityValidator
        spark = SchemaGenerator.typemap
        avro = dict(AVRO_TYPEMAP)
        tmap = EntityValidator.type_description
        for k in avro:
            if isinstance(avro[k], dict):
                avro[k] = "Logical Type"

        self.print("Cannonical types and mapping:\n")
        t_map = [
            {
                "Name": k,
                "Arrow": arrow.get(k, lambda t: "N/A")(None),
                "Avro": avro.get(k, "N/A"),
                "Spark": spark.get(k, "N/A"),
                "Description": v,
            }
            for k, v in tmap.items()
        ]
        self.tabulate(t_map)

    def init_parser(self):
        """initialize argparse parser"""
        self.init_sub_parser("maintain schema")

        # print_types
        _ = self.sub_parser.add_parser(
            "show_types", help="print list of canonnical types"
        )

        # infer
        parser_infer = self.sub_parser.add_parser("infer", help="infer schema from existing data")
        options.add_option_defaults(parser_infer)
        options.add_options_sampling(parser_infer)
        options.add_options_inputs(parser_infer)

        # ls
        parser_ls = self.sub_parser.add_parser("ls", help=self.do_ls.__doc__)
        options.add_option_defaults(parser_ls)
        options.add_option_long_format(parser_ls)

        # print
        parser_print = self.sub_parser.add_parser("print", help=self.do_print.__doc__)
        options.add_option_defaults(parser_print)
        options.add_option_entity(parser_print)

        # rm
        parser_rm = self.sub_parser.add_parser("rm", help=self.do_rm.__doc__)
        options.add_option_defaults(parser_rm)
        options.add_option_entity(parser_rm)
        options.add_option_yes(parser_rm)

        # export
        parser_export = self.sub_parser.add_parser(
            "export", help=self.do_export.__doc__
        )
        options.add_option_defaults(parser_export)
        parser_export.add_argument(
            "-t", "--type", choices=[
                'pyarrow',
                'dot',
                'model',
                'spark',
                'sql.create',
                'sql.select',
            ],
            help="export type", required=True
        )
        parser_export.add_argument(
            "--dialect", default=None, choices=sa.VALID_DIALECTS,
            help="Dialects for SQL schema exports"
        )
        parser_export.add_argument(
            "-o", "--output", help="Export to file", default=None
        )
        options.add_option_glob(parser_export)

        # grep
        parser_grep = self.sub_parser.add_parser("grep", help=self.do_grep.__doc__)
        options.add_option_defaults(parser_grep)
        options.add_option_long_format(parser_grep)
        options.add_option_pattern(parser_grep)

        # fgrep
        parser_fgrep = self.sub_parser.add_parser("fgrep", help=self.do_fgrep.__doc__)
        options.add_option_defaults(parser_fgrep)
        options.add_option_long_format(parser_fgrep)
        options.add_option_pattern(parser_fgrep)

        # h5-ls
        parser_h5_ls = self.sub_parser.add_parser(
            "h5-ls",
            help=self.do_h5_ls.__doc__
        )
        options.add_option_defaults(parser_h5_ls)
        options.add_option_long_format(parser_h5_ls)
        options.add_option_connection_name(parser_h5_ls)
        # group_h5_conn = parser_h5_ls.add_mutually_exclusive_group(required=False)
        # options.add_option_connection_name_opt(group_h5_conn)
        # options.add_arg_uri(group_h5_conn, "?")

        # h5-reflect
        parser_h5_reflect = self.sub_parser.add_parser(
            "h5-reflect",
            help=self.do_h5_reflect.__doc__
        )
        options.add_options_minimal(parser_h5_reflect)
        options.add_option_connection_name_opt(parser_h5_reflect)
        options.add_arg_uri(parser_h5_reflect, n=1)
        options.add_option_entity_optional(parser_h5_reflect)

        # import
        parser_import = self.sub_parser.add_parser("import", help=self.do_import.__doc__)
        options.add_options_minimal(parser_import)
        options.add_option_alternate_model(parser_import)
        options.add_arg_entities(parser_import)

        # sql-ls
        parser_sql_ls = self.sub_parser.add_parser(
            "sql-ls",
            help=self.do_sql_ls.__doc__
        )
        options.add_option_defaults(parser_sql_ls)
        options.add_option_connection_name(parser_sql_ls)
        options.add_option_long_format(parser_sql_ls)
        # options.add_option_connection_uri(parser_sql_ls)

        # sql-reflect
        parser_sql_reflect = self.sub_parser.add_parser("sql-reflect",
                                                        help=self.do_sql_reflect.__doc__)
        options.add_option_defaults(parser_sql_reflect)
        options.add_option_connection_name(parser_sql_reflect)
        options.add_option_glob(parser_sql_reflect)
        options.add_option_append(parser_sql_reflect)

        super().parse_args()
