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
import os
import argparse
import configparser
import logging

from . import exceptions


logger = logging.getLogger(__name__)


class Repository(object):
    """
    Repository object

    Data repository that is shared between objects in an application.

    This repository stores:

    * configuration
    * arguments
    * custom data
    """
    def __init__(self, config=None, arguments=None, properties=None, data=None):
        self.__config = config
        self.__arguments = arguments
        if properties is None:
            self._properties = {}
        else:
            self._properties = properties
        self.__data = data

    def __get_arguments(self):
        return self.__arguments

    def __set_arguments(self, args):
        self.__arguments = args

    arguments = property

    def __get_config(self):
        return self.__config

    def __set_config(self, value):
        self.__config = value

    config = property(__get_config, __set_config, None, None)

    def __get_properties(self):
        return self._properties

    def __set_properties(self, value):
        if value is None:
            value = {}
        self._properties = value

    properties = property(__get_properties, __set_properties, None, None)

    def __get_data(self):
        return self.__data

    def __set_data(self, value):
        self.__data = value

    data = property(__get_data, __set_data, None, None)


class ArgumentsMixin(object):
    """
    Mixin that create an arguments property.

    The arguments property should be an argparse instance.

    :param arguments: argparse instance
    """

    # def __init__(self, argument_parser, **kwds):
    #    super().__init__(**kwds)

    def __get_args(self):
        """Return ArgParser instance."""
        return self.repository.arguments

    def __set_args(self, options):
        self.repository.arguments = options

    arguments = property(__get_args, __set_args)


class InitArgumentsMixin(ArgumentsMixin):
    """
    Initialize an argumentparser with the following arguments:

    * `--config` configuration FILENAME
    * `-v`: verbose on
    * `-d`: debug on
    * `-l`: loglevel
    * `args` arguments

    To add additional arguments, override the `_init_additional_args` method.
    """
    def __init__(self, argument_parser=None, **kwds):
        # self.repository.arguments = argument_parser.parse_args()
        self.repository.argument_parser = self.__init_argument_parser(argument_parser)
        self.arguments = self.argument_parser.parse_args()
        super().__init__(**kwds)

    def init_additional_args(self, argument_parser):
        """
        override this method to add additional arguments.

        :param argument_parser: argument parser instance
        """
        pass

    def __init_argument_parser(self, argument_parser):
        """
        Initialise argument parser object.

        :param argument_parser: ArgumentParser instance.
        """
        if argument_parser is None:
            argument_parser = argparse.ArgumentParser()
        else:
            assert isinstance(argument_parser, argparse.ArgumentParser)
            argument_parser = argument_parser

        argument_parser.add_argument(
            "-v", "--verbose",
            action="store_true",
            dest="verbose",
            default=False,
            help='Log INFO and higher events to terminal'
        )

        argument_parser.add_argument(
            "--debug",
            action="store_true",
            dest="debug",
            default=False,
            help="log DEBUG and higher events to terminal."
        )

        argument_parser.add_argument(
            "--loglevel",
            dest="loglevel",
            default='error',
            help="Log level (to log file): debug, info, warning, error or " +
            "critical. Default is '%(default)s'."
        )

        argument_parser.add_argument(
            dest="args",
            help="Arguments to process",
            type=str,
            metavar="args",
            nargs='*'
        )

        argument_parser.add_argument(
            "--config",
            dest="config",
            default=None,
            help="Configuration file FILENAME."
        )

        self.init_additional_args(argument_parser)
        return argument_parser

    @property
    def argument_parser(self):
        return self.repository.argument_parser


class ConfigMixin(object):
    """
    mixin that add a confi property

    The config property represent an configparser instance.
    """

    def __init__(self, config=None, **kwds):
        self.repository.config = config
        super().__init__(**kwds)

    # config property =========================================================
    def __get_config(self):
        """Return ConfigurationParser instance."""
        return self.repository.config

    def __set_config(self, config):
        self.repository.config = config

    config = property(__get_config, __set_config)


class InitConfigMixin(ConfigMixin):
    """
    initializes configparser

    Initializes configparser from file specified in arguments or
    specified in parameters.

    :param config: configparser instance (or None)
    :param default_config: file location for default config e.g ~/.appconfig.ini
    """

    def __init__(self, config=None, default_config=None, **kwds):
        _config = self.__init_config_parser(config, default_config)
        ConfigMixin.__init__(self, _config, **kwds)

    def __init_config_parser(self, config, default_config):
        """
        Initialise the configuration parser

        Args:
            config: ConfigParser instance
            default_config: Default configuration

        Returns:
            ConfigParser instance.
        """
        if config is None:
            if self.arguments.config is not None:
                config = configparser.ConfigParser()
                config_filename = os.path.expanduser(self.arguments.config)
                config.readfp(open(config_filename))
            elif default_config is not None:
                # Config file not specified with -f, execute if a defualt file is specified.
                config_filename = os.path.expanduser(default_config)
                config = configparser.ConfigParser()
                config.readfp(open(config_filename))
            else:
                raise exceptions.DKitConfigException("No valid configration file specified.")
            return config
        else:
            return config


class ConfiguredObject(object):
    "Base class which implements the config reader"

    def __init__(self, repository=None, **kwds):
        if repository is None:
            self.repository = Repository()
        else:
            self.repository = repository
        super().__init__()

    # ==========================================================================
    #  Properties
    # ==========================================================================

    # Repository ==============================================================
    # def __get_repository(self):
    #    return self.repository

    # def __set_repository(self, value):
    #    self.repository = value

    # repository = property(__get_repository, __set_repository)

    # data property ===========================================================
    def __get_data(self):
        """Return repository data.  This can be a database connection."""
        return self.repository.data

    def __set_data(self, value):
        self.repository.data = value

    data = property(__get_data, __set_data)

    # properties property =====================================================
    def __get_properties(self):
        """Return repository properties."""
        return self.repository.properties

    properties = property(__get_properties)


class ConfiguredApplication(ConfiguredObject):
    """
    base class for application instances.

    use the following mixins (add in this sequence):

    * `InitArgumentsMixin`;
    * `InitConfigMixin`;

    .. include:: ../../examples/example_configured_application.py
        :literal:

    """
    def __init__(self, repository=None, **kwds):
        super().__init__(repository, **kwds)


class ConsoleArgumentsApplication(ConfiguredApplication, InitArgumentsMixin):
    """
    Command line application with:

    * Logging
    * Arguments
    """
    pass


class ConsoleApplication(ConfiguredApplication, InitArgumentsMixin, InitConfigMixin):
    pass
