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
Maintain transforms
"""

from . import module, defaults, options
from dkit.etl import transform, model
from dkit.data import iteration


class TransformModule(module.MultiCommandModule):

    def do_create(self):
        """
        create transform from entity entity
        """
        services = self.load_services()
        etl_model = services.model

        # trn_name -> transform name
        if not self.args.transform:
            trn_name = "trn_{}".format(self.args.entity)
        else:
            trn_name = self.args.transform

        # create the transform
        new_transform = model.Transform(
            transform.FormulaTransform.from_entity(
                etl_model.entities[self.args.entity],
                key_case=self.args.case
            )
        )

        etl_model.transforms[trn_name] = new_transform
        services.save_model_file(etl_model, self.args.model_uri)

    def do_uuid(self):
        """
        add UUID (and convert between formats if requested)
        """
        self.push_to_uri(
            self.args.output,
            iteration.iter_add_id(
                self.input_stream(self.args.input),
                key=self.args.fieldname
            )
        )

    def do_ls(self):
        """list transforms and exit"""
        services = self.load_services()
        model = services.model
        transforms = [
            {"name": k, "# rules": len(v)}
            for k, v in model.transforms.items()
            if not k.startswith("--")
        ]
        self.tabulate(transforms)

    def do_print(self):
        """print transform rules"""
        etl_model = self.load_services().model
        data = [
            {"name": k, "rule": v}
            for k, v in etl_model.transforms[self.args.transform].items()
            if not k.startswith("__")
        ]
        self.tabulate(data)

    def do_rm(self):
        """remove transform"""
        if self.get_confirmation("remove transform: [{}]".format(self.args.transform)):
            services = self.load_services()
            etl_model = services.model
            del etl_model.transforms[self.args.transform]
            services.save_model_file(etl_model, self.args.model_uri)

    def init_parser(self):
        """initialize argparse parser"""
        self.init_sub_parser("maintain and run transforms")

        # create
        parser_create = self.sub_parser.add_parser(
            "create",
            help=self.do_create.__doc__
        )
        options.add_option_defaults(parser_create)
        options.add_option_transform_name(parser_create)
        options.add_option_entity(parser_create)
        parser_create.add_argument("--case", help="convert field name case",
                                   choices=defaults.CASE_TRANSFORMS, default="same")

        # ls
        parser_ls = self.sub_parser.add_parser(
            "ls",
            help=self.do_ls.__doc__,
        )
        options.add_option_defaults(parser_ls)

        # print
        parser_print = self.sub_parser.add_parser(
            "print",
            help=self.do_print.__doc__,
        )
        options.add_option_defaults(parser_print)
        options.add_option_transform_name(parser_print)

        # rm
        parser_rm = self.sub_parser.add_parser(
            "rm",
            help=self.do_rm.__doc__
        )
        options.add_option_defaults(parser_rm)
        options.add_option_transform_name(parser_rm)
        options.add_option_yes(parser_rm)

        # uuid
        parser_uuid = self.sub_parser.add_parser(
            "uuid",
            help=self.do_uuid.__doc__
        )
        options.add_option_defaults(parser_uuid)
        parser_uuid.add_argument("--field-name", dest="fieldname", default="uuid",
                                 help="UUID Field name.")
        options.add_options_inputs(parser_uuid)
        options.add_option_output_uri(parser_uuid)

        super().parse_args()
