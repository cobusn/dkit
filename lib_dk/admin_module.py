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
configuration file management subsystem
"""
from . import module, options
from dkit.etl import model


class AdminModule(module.MultiCommandModule):
    """
    Maintain application configuration files.
    Configuration files is used for storing encryption keys.
    """
    def do_convert(self):
        """
        convert model between codec file formats
        """
        services = self.load_services()
        services.save_model_file(
            services.model,
            self.args.destination
        )

    def do_init_config(self):
        """
        initialize configuration file
        """
        model.ETLServices.init_config(self.args.config_uri)

    def do_init_model(self):
        """
        initialize configuration file
        """
        model.ETLServices.init_model_file(self.args.model_uri)

    def init_parser(self):
        """initialize argparse parser"""
        self.init_sub_parser(self.doc_string)

        # convert model
        parser_convert = self.sub_parser.add_parser(
            "convert",
            help=self.do_convert.__doc__
        )
        options.add_option_defaults(parser_convert)
        parser_convert.add_argument("--destination", required=True, help="destination model")

        # init_config
        parser_init_config = self.sub_parser.add_parser(
            "init_config",
            help="initialize configuration file (~/.dk.conf)"
        )
        options.add_option_config(parser_init_config)

        # init_model
        parser_init_model = self.sub_parser.add_parser(
            "init_model",
            help="initialize configuration file"
        )
        options.add_option_model(parser_init_model)

        super().parse_args()
