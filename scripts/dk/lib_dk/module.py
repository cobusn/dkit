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
#
import argparse
import codecs
import configparser
import getpass
import itertools
import operator
import os
import logging
import textwrap
from typing import Dict, List, Iterable

import tabulate

from . import defaults
from dkit import exceptions
from dkit.data import iteration
from dkit.data import filters
from dkit.etl import transform, model
from dkit.shell import console
from dkit.utilities import cmd_helper, log_helper


class Module(object):

    def __init__(self, arguments):
        self.arguments = arguments
        self.parser = argparse.ArgumentParser()
        self.init_parser()
        self.table_style = defaults.TABLE_STYLE
        self.__config = None
        # the below is only loaded when required
        self.current_services = None

    def columnize(self, data):
        """print list like data in columns"""
        if hasattr(self.args, "long") and self.args.long:
            for item in data:
                self.print(item)
        else:
            width, _ = console.get_terminal_size()
            console.columnize(data, displaywidth=width)

    def load_services(self, services_class=model.ETLServices):
        """
        load services
        """
        return services_class.from_file(
            self.args.model_uri,
            self.config
        )

    @property
    def config_file_names(self):
        """
        user configuration filename

        raises:
            - CkitConfigException
        """
        global_file = os.path.expanduser(defaults.GLOBAL_CONFIG_FILE)
        config_paths = []
        # add global config file if exist
        if os.path.exists(global_file):
            config_paths.append(global_file)

        # add specified config file if exist
        if self.args.config_uri is not None:
            config_paths.append(self.args.config_uri)

        # else add default local config file
        elif os.path.exists(defaults.LOCAL_CONFIG_FILE):
            config_paths.append(defaults.LOCAL_CONFIG_FILE)

        # raise exception if no confif file
        if len(config_paths) == 0:
            raise exceptions.DKitConfigException("No Valid configuration files found")
        return config_paths

    @property
    def config(self):
        if self.__config is None:
            config = configparser.ConfigParser()
            config.read(self.config_file_names)
            self.__config = config
        return self.__config

    @property
    def log_trigger(self):
        if self.args.log_trigger is None:
            if self.config.has_option("DEFAULT", "log_trigger"):
                return self.config.getint("DEFAULT", "log_trigger")
            else:
                return defaults.DEFAULT_LOG_TRIGGER
        else:
            return self.arts.log_trigger

    def get_confirmation(self, prompt):
        return cmd_helper.confirm(prompt, False) if not self.args.yes else True

    def get_password(self, prompt="Password: "):
        """get password interactively"""
        return getpass.getpass(prompt)

    def init_logging(self):
        """initialize logging pipeline"""
        if "verbose" in self.args and self.args.verbose:
            log_helper.init_stderr_logger(level=logging.INFO)
        elif "warning" in self.args and self.args.warning:
            log_helper.init_stderr_logger(level=logging.WARN)
        elif "debug" in self.args and self.args.debug:
            log_helper.init_stderr_logger(level=logging.DEBUG)
        else:
            log_helper.init_null_logger()

    def init_parser(self):
        """Initialize parser"""
        raise NotImplementedError

    def do_output(self, iter_out):
        if hasattr(self.args, "table") and self.args.table:
            self.tabulate(iter_out)
        else:
            self.push_to_uri(self.args.output, iter_out)

    def parse_args(self):
        self.args = self.parser.parse_args(self.arguments)
        self.init_logging()

    def print(self, *data):
        """print"""
        print(*data)

    def run(self):
        raise NotImplementedError

    def tabulate(self, data, floatfmt="0.2f"):
        """tabulate data"""
        if hasattr(self.args, "width") and self.args.width > 0:
            w = self.args.width
            data = (
                lambda x: {k: str(v)[:w] for k, v in x.items()}
                for row in data
            )

        if hasattr(self.args, "transpose") and self.args.transpose is True:
            for row in data:
                for field in sorted(row.keys()):
                    value = row[field]
                    self.print("{:30s}: {}".format(str(field)[:30], value))
                self.print("\n")
        else:
            self.print(tabulate.tabulate(
                data,
                headers="keys",
                tablefmt=self.table_style,
                floatfmt=floatfmt
            ))

    @property
    def doc_string(self):
        """class docstring"""
        return textwrap.dedent(self.__doc__)

    def input_stream_raw(self, uri_list: List[str], fields=None) -> Iterable[dict]:
        """
        raw input stream without schema or transform applied
        """
        model_services = model.ETLServices.from_file(
            self.args.model_uri,
            self.args.config_uri
        )
        delimiter = codecs.decode(self.args.delimiter, "unicode_escape")

        # determine field list
        if fields is not None:
            field_list = fields
        elif hasattr(self.args, "fields"):
            field_list = self.args.fields
        else:
            field_list = None

        # load CSV headings if specified
        if hasattr(self.args, "headings") and self.args.headings:
            with open(self.args.headings, "rt") as infile:
                headings = [i.strip() for i in list(infile)]
        else:
            headings = None

        # where clause
        where_clause = self.args.where if hasattr(self.args, "where") else None
        for the_uri in uri_list:
            with model_services.model.source(
                uri=the_uri,
                skip_lines=self.args.skip_lines,
                field_names=field_list,
                where_clause=where_clause,
                headings=headings,
                delimiter=delimiter,
                work_sheet=self.args.work_sheet
            ) as in_data:
                yield from in_data

    def input_stream(self, uri_list: List[str], fields=None) -> Iterable[dict]:
        """
        open and return an input stream.abs

        Optionally apply entity schemas, filter or transform
        """

        # if a filter is defined we need to make sure the filter fields
        # are added to the fields extracted.
        if hasattr(self.args, "filter") and self.args.filter is not None:
            exp_filter = filters.ExpressionFilter(self.args.filter)
            # ensure we extract filter fields
            if fields is not None:
                fields += exp_filter.parsed_variable_names

        _iter_in = self.input_stream_raw(uri_list, fields)
        services = self.load_services()

        # apply schema
        if hasattr(self.args, "entity") and self.args.entity is not None:
            _schema = services.model.entities[self.args.entity]
            _iter_in = _schema(_iter_in)

        # apply filter
        if hasattr(self.args, "filter") and self.args.filter is not None:
            # exp_filter should already be instantiated above..
            _iter_in = filter(exp_filter, _iter_in)

        # apply transform
        if hasattr(self.args, "transform") and self.args.transform is not None:
            dict_transform = services.model.transforms[self.args.transform]
            t = transform.FormulaTransform(dict_transform)
            _iter_in = t(_iter_in)

        # sort
        if hasattr(self.args, "sort") and self.args.sort is not None:
            getter = operator.itemgetter(*self.args.sort)
            _iter_in = sorted(
                _iter_in,
                key=getter,
                reverse=self.args.reversed
            )

        if hasattr(self.args, "n") and self.args.n > 0:
            yield from itertools.islice(_iter_in, self.args.n)
        else:
            yield from _iter_in

    def input_stream_sampled(self, uri_list: List[str]) -> Iterable[Dict]:
        """
        open a sampled input stream
        """
        the_iterator = self.input_stream(uri_list)
        yield from iteration.iter_sample(
            the_iterator,
            self.args.sample_probability,
            self.args.sample_size
        )

    def push_to_uri(self, uri_name, the_iterable):
        """
        write output to sink
        """
        services = self.load_services()
        with services.model.sink(uri_name) as dest:
            dest.process(the_iterable)


class UniCommandModule(Module):
    pass


class MultiCommandModule(Module):
    """module that can process multiple commands"""

    def run(self):
        available_methods = [i for i in dir(self) if i.startswith("do_")]
        method = "do_{}".format(self.args.command.replace("-", "_"))
        candidate_methods = [i for i in available_methods if i.startswith(method)]
        if len(candidate_methods) != 1:
            self.print("Unrecognized command")
            self.parser.print_help()
            exit(1)
        else:
            getattr(self, candidate_methods[0])()

    def init_sub_parser(self, description=None):
        self.sub_parser = self.parser.add_subparsers(
            dest='command',
            description=description,
        )


class CRUDModule(MultiCommandModule):
    """
    basis for CRUD operations in command line interface
    """
    def __init__(self, arguments, entity_name):
        self.entity_name = entity_name
        super().__init__(arguments)

    def do_add(self, name, instance):
        raise NotImplementedError

    def do_ls(self):
        """list and exit"""
        model = self.load_services().model
        obj_type = model.schema[self.entity_name]
        container = getattr(model, self.entity_name)
        if hasattr(self.args, "long") and self.args.long is True:
            listing = obj_type.get_listing(container)
            self.tabulate(listing)
        else:
            for name in container:
                self.print(name)

    def do_print(self, name):
        """print details for item"""
        model = self.load_services().model
        container = getattr(model, self.entity_name)
        item = container[name]
        self.print(f"name: {name}")
        self.tabulate([{"item": k, "value": v} for k, v in item.as_dict().items()])

    def do_rm(self, name):
        """remove item"""
        services = self.load_services()
        container = getattr(services.model, self.entity_name)
        del container[name]
        services.save_model_file(
            services.model,
            self.args.model_uri
        )
