#
# Copyright (C) 2016  Cobus Nel
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#

"""
ETL utilities
Set of utilites for data extraction and schema managment.

Usage

etl infer -j|c|t <<filename>>
etl ingest -j <filename> -m model.yml~sales -d "sqlite:////absolute/path/to/foo.db~sales"
etl create -m model.yml -d "sqlite:////absolute/path/to/foo.db~sales"
etl create -m sales:model.yml -d "sqlite:////absolute/path/to/foo.db~sales"
etl translate -i csv:unput.csv -o jsonl:stdout -m sales:jsonl.model
etl infer -i csv|input.json

* infer: infer model from input file
* -j <filename>: jsonl input
* -c <filename>: csv input
* -t <filename>: tsv input
* -d: dsn
* -m <<entity:model file>>: specify model yaml
* -s visual summary
* -y summary
* -p output (xlsx|json|csv|xml|tsv)
* -e "," separator
* -o <<output filename>>
"""

import sys; sys.path.insert(0, "..")
import argparse
import yaml
import tempfile
import os
import shutil
import codecs

from dkit import base
from dkit.data import manipulate
from dkit.etl import (schema, transform, utilities)


class MetlDriver(base.ConsoleArgumentsApplication):
    """
    etl data model utility

    * infer data model from file;
    * create databases from model;
    * ingest data into a database using model;
    * translate between formats using model.
    """


    def __check_argument_group(self):
        """
        check that path argument starts with `/root`
        """
        group = self.arguments.group
        if not group.startswith("/"):
            self.argument_parser.error("path argument should start with '/'")

    def run(self):
        """
        execute the appropriate module
        """
        delegation_map = {
            "h5.create_table": self.do_hdf5_create_table,
            "h5.ingest": self.do_hdf5_ingest,
            "h5.query": self.do_hdf5_query,
            "transform": self.do_transform,
            "transform.create": self.do_transform_create,
            "convert": self.do_convert,
            "uuid": self.do_uuid,
        }

        command = self.arguments.command
        if command in list(delegation_map.keys()):
            try:
                delegation_map[command]()
            except FileNotFoundError as E:
                self.logger.critical(str(E))

    def do_hdf5_create_table(self):
        """
        Create hdf5 model from definition file.
        """
        from cetl.extensions import ext_tables
        self.__check_argument_group()

        file_name = self.arguments.db_name
        entity_name = self.arguments.entity
        node_name = self.arguments.name if self.arguments.name else entity_name
        group_name = self.arguments.group
        title = self.arguments.title

        yaml_data = yaml.load(self.arguments.yaml.read())
        validator = schema.SchemaValidator.from_dict(yaml_data['entities'][entity_name])
        model = ext_tables.PyTablesModelFactory().create_model(validator)

        accessor = ext_tables.PyTablesAccessor(file_name)
        accessor.create_table(group_name, node_name, model, title)

        del accessor  # will close the file

    def do_hdf5_ingest(self):
        """
        Infer model from input file
        """
        from cetl.extensions import ext_tables
        if not self.arguments.entity_name:
            node = self.arguments.entity
        else:
            node = self.arguments.entity_name
        skip_lines = self.arguments.skip_lines
        source_type = None if self.arguments.input_type == "auto" else self.arguments.input_type
        file_list = self.arguments.file_names
        yaml = self.arguments.yaml.read()
        db_name = self.arguments.db_name
        group = self.arguments.group
        delimiter = codecs.decode(self.arguments.delimiter, "unicode_escape")

        validator = schema.SchemaValidator.from_yaml(yaml, self.arguments.entity)
        iter_src = utilities.SourceIteratorFactory(file_list, source_type, skip_lines=skip_lines,
                                                   delimiter=delimiter)
        accessor = ext_tables.PyTablesAccessor(db_name)
        the_sink = ext_tables.PyTablesSink(accessor, group, node, logger=self.logger)

        if self.arguments.transform is None:
            iter_coerce = transform.CoerceTransform(iter_src, validator)
            the_sink.process(iter_coerce)
        else:
            iter_trans = transform.FormulaTransform.from_yaml(iter_src, yaml,
                                                             self.arguments.transform)
            iter_coerce = transform.CoerceTransform(iter_trans, validator)
            the_sink.process(iter_coerce)

    def do_hdf5_query(self):
        """
        print hdf5 content as json
        """
        from cetl.extensions import ext_tables
        if not self.arguments.entity_name:
            node_name = self.arguments.entity
        else:
            node_name = self.arguments.entity_name
        db_name = self.arguments.db_name
        group = self.arguments.group
        where = self.arguments.where

        accessor = ext_tables.PyTablesAccessor(db_name)
        iter_src = ext_tables.PyTablesSource(accessor, group, node_name, logger=self.logger,
                                             where_clause=where)
        the_sink = utilities.sink_factory(self.arguments.output, self.logger)
        the_sink.process(iter_src)



    def init_additional_args(self, argument_parser):
        ap = argument_parser
        sp = ap.add_subparsers(dest='command')
        #
        # h5.create_table
        #
        parser_create = sp.add_parser("h5.create_table")
        parser_create.add_argument('--db', dest="db_name", help="HDF5 database.")
        parser_create.add_argument('--title', dest="title", help="table description",
                                   default="machine generated")
        parser_create.add_argument('-g', '--group', dest="group", help="hdf5 group.", default="/")
        parser_create.add_argument('-e', dest="entity", help="entity name in model", required=True)
        parser_create.add_argument('-n', dest="name",
                                   help="entity name in db if different than in model",
                                   default=False)
        parser_create.add_argument('-y', dest="yaml", help="yaml file [default is stdin]",
                                   type=argparse.FileType('r'),
                                   default=sys.stdin)
        #
        # h5.ingest
        #
        parser_ingest = sp.add_parser("h5.ingest")
        parser_ingest.add_argument('-e', dest="entity", help="entity name", required=True)
        parser_ingest.add_argument('-t', dest="transform", help="apply this transform",
                                   default=None)
        parser_ingest.add_argument('--db', dest="db_name", help="HDF5 database.")
        parser_ingest.add_argument('-g', '--group', dest="group", help="hdf5 group.", default="/")
        parser_ingest.add_argument('-n', dest="entity_name",
                                   help="entity name in db if different than in model",
                                   default=False)
        parser_ingest.add_argument('-i', "--input-type", dest="input_type",
                                   help='input type. default is auto. [auto, xlsx, json, csv]',
                                   default="auto")
        parser_ingest.add_argument('-y', dest="yaml", help="yaml file [default is stdin]",
                                   type=argparse.FileType('r'), default=sys.stdin)
        self.__add_argparse_csv_options(parser_ingest)
        self.__add_argparse_input_files(parser_ingest)

        #
        # h5.query
        #
        parser_query_h5 = sp.add_parser("h5.query")
        parser_query_h5.add_argument('-e', dest="entity", help="entity name", required=True)
        parser_query_h5.add_argument('--db', dest="db_name", help="HDF5 database.", required=True)
        parser_query_h5.add_argument('-g', '--group', dest="group", help="hdf5 group.", default="/")
        parser_query_h5.add_argument('-n', dest="entity_name",
                                     help="entity name in db if different than in model",
                                     default=False)
        parser_query_h5.add_argument('--where', dest="where", help="where clause", default=None)
        self.__add_argparse_output_options(parser_query_h5)


def main():
    MetlDriver().run()


if __name__ == "__main__":
    main()
