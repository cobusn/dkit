from . import module, options
from dkit.data import aggregation as agg
from dkit import exceptions
import argparse


class GroupByAction(argparse.Action):

    def __call__(self, parser, namespace, values, option_string=None):
        if hasattr(namespace, "group_by_operations"):
            operations = getattr(namespace, "group_by_operations")
        else:
            operations = []
            setattr(namespace, "group_by_operations", operations)
        aggregator_class = agg.MAP_NON_PARAMETRIC_FUNCTIONS[self.dest]
        if ":" in values:
            v = values.split(":")
            aggregator = aggregator_class(v[0])
            aggregator.alias(v[1])
        else:
            aggregator = aggregator_class(values)
        operations.append(aggregator)


class AggModule(module.UniCommandModule):

    def get_aggregator(self):
        """build dkit.data.aggregator.Aggregate object from options"""
        aggregator = agg.Aggregate()

        # Check that at least one operation has been specified
        if not hasattr(self.args, "group_by_operations"):
            raise exceptions.CkitApplicationException("No group by operation specified")

        # set group by keys
        aggregator = aggregator + agg.GroupBy(*self.args.group_by)

        for operation in self.args.group_by_operations:
            aggregator = aggregator + operation

        return aggregator

    def run(self):
        aggr = self.get_aggregator()
        field_list = aggr.groupby_keys + [i.target for i in aggr.aggregations]
        self.args.fields = field_list
        if self.args.table is True:
            a = list(aggr(self.input_stream(
                    self.args.input, fields=aggr.required_fields)
                ))
            self.tabulate(a)
        else:
            self.push_to_uri(
                self.args.output,
                aggr(self.input_stream(
                    self.args.input,
                    fields=aggr.required_fields
                ))
            )

    def init_parser(self):
        options.add_option_defaults(self.parser)
        options.add_options_inputs(self.parser)
        options.add_option_output_uri(self.parser)
        self.parser.add_argument("-g", "--group_by", dest="group_by", action="append",
                                 default=[], help="add group_by field")
        for name, class_obj in agg.MAP_NON_PARAMETRIC_FUNCTIONS.items():
            self.parser.add_argument(
                f"--{name}",
                dest=name,
                action=GroupByAction,
                help=class_obj.__doc__
            )
        options.add_option_tabulate(self.parser)
        self.parse_args()
