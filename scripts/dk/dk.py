#!/usr/bin/env python3

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

import argparse
import os
import sys
import textwrap

from lib_dk import defaults

from dkit import exceptions as dkit_exceptions, __version__
from dkit.utilities import log_helper


VERSION = __version__


class DataKit(object):
    """

    etl MODULE options

    maintenance modules:
        admin        maintain configuration and models
        connections  maintain connections
        endpoints    maintain endpoints
        queries      maintain queries
        mapping      maintain entity relation mapping
        schemas      maintain schemas
        transforms   mintain transforms
        XML          maintain XML rules

    action modules:
        run          run etl or query
        xplore       explore data

    environmental variables:
        set DK_DEBUG to 'True' to disable exception handling

    (use dk MODULE -h for details)
    """

    modules = sorted(["admin", "connections", "endpoints", "run", "queries",
                      "mapping", "schemas", "transforms", "xplore", "XML"])

    def __init__(self, arguments):
        self.arguments = arguments
        self.logger = log_helper.stderr_logger(logger_name="dk_logger")

    def print(self, string):
        print(string)

    def do_admin(self):
        from lib_dk import admin_module
        admin_module.AdminModule(self.arguments[2:]).run()

    def do_connections(self):
        from lib_dk import connections_module
        connections_module.ConnectionsModule(self.arguments[2:]).run()

    def do_endpoints(self):
        from lib_dk import endpoints_module
        endpoints_module.EndpointsModule(self.arguments[2:]).run()

    def do_run(self):
        from lib_dk import run_module
        run_module.RunModule(self.arguments[2:]).run()

    def do_queries(self):
        from lib_dk import queries_module
        queries_module.QueriesModule(self.arguments[2:]).run()

    def do_mapping(self):
        from lib_dk import relations_module
        relations_module.RelationsModule(self.arguments[2:]).run()

    def do_schemas(self):
        from lib_dk import schema_module
        schema_module.SchemaModule(self.arguments[2:]).run()

    def do_transforms(self):
        from lib_dk import transform_module
        runner = transform_module.TransformModule(self.arguments[2:])
        runner.run()

    def do_xplore(self):
        from lib_dk import explore_module
        runner = explore_module.ExploreModule(self.arguments[2:])
        runner.run()

    def do_XML(self):
        from lib_dk import xml_module
        xml_module.XMLModule(self.arguments[2:]).run()

    def get_method(self):
        """get module name"""
        parser = argparse.ArgumentParser(
            prog="dk",
            description="data processing toolkit",
            usage=textwrap.dedent(self.__doc__)
        )
        parser.add_argument("module", help="module to execute")
        args = parser.parse_args(self.arguments[1:2])

        candidates = [i for i in self.modules if i.startswith(args.module.strip())]
        if len(candidates) == 1:
            return "do_{}".format(candidates[0])
        elif len(candidates) > 1:
            self.print("Ambiguous options: {}".format(", ".join(candidates)))
            parser.print_help()
            exit(1)
        else:
            self.print("Unrecognized command")
            parser.print_help()
            exit(1)

    def run(self):
        method = self.get_method()
        if bool(os.environ.get("DK_DEBUG", False)):
            getattr(self, method)()
        else:
            try:
                getattr(self, method)()
            except KeyError as err:
                print(defaults.MSK_ERR_KEYERROR.format(str(err)))
            except dkit_exceptions.CkitApplicationException as err:
                print(err)
            except Exception as err:
                print(f"An exception occurred: {err.__class__.__name__}: {err}")


def main():
    DataKit(sys.argv).run()


if __name__ == "__main__":
    main()
