#  shcema for report.yaml
import cerberus
import yaml

from ..exceptions import CkitValidationException
from ..messages import MSG_0019
from ..utilities import log_helper as lh


try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


report_schema = """
configuration:
    required: True
    type: dict
    schema:
        plot_folder:
            required: True
            type: string
latex:
    type: list
    schema:
        type: string
styles:
    type: list
    schema:
        type: string
documents: &_dict
    type: dict
    keyschema:
        type: string
    valueschema:
        type: string
presentations: *_dict
code: *_dict
data: *_dict
variables:
    type: dict
    required: False
    keyschema:
        type: string
    valueschema:
        type:
            - string
            - boolean
            - float
            - integer
            - list
            - dict
"""


class SchemaValidator(object):
    """
    load Cerberus schema from yaml object

    log any errors

    arguments:
        schema_yaml: yaml formatted schema
        logger: logger instance, default to stderr
    """
    def __init__(self, schema_yaml: str, logger=None):
        self.schema = yaml.load(schema_yaml, Loader=Loader)
        self.logger = logger if logger else lh.stderr_logger()
        self.validator = cerberus.Validator(self.schema)

    def validate(self, instance):
        """
        raises CkitValidationException
        """
        validated = self.validator.validate(instance)
        if not validated:
            for k, error in self.validator.errors.items():
                self.logger.error(f"element {k}: {str(error)}")
            raise CkitValidationException(MSG_0019)

    def __call__(self, instance):
        self.validate(instance)


if __name__ == "__main__":
    print(report_schema())
