#  shcema for report.yaml
import cerberus
import yaml
import logging

from ..exceptions import CkitValidationException
from ..messages import MSG_0019


try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

logger = logging.getLogger(__name__)


report_schema = """
configuration:
    required: True
    type: dict
    schema:
        plot_folder:
            required: True
            type: string
        template_folder:
            required: True
            type: string
        version:
            required: True
            type: integer
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

    arguments:
        schema_yaml: yaml formatted schema
        logger: logger instance, default to stderr
    """
    def __init__(self, schema_yaml: str):
        self.schema = yaml.load(schema_yaml, Loader=Loader)
        self.validator = cerberus.Validator(self.schema)

    def validate(self, instance):
        """
        raises CkitValidationException
        """
        validated = self.validator.validate(instance)
        if not validated:
            for k, error in self.validator.errors.items():
                logger.error(f"element {k}: {str(error)}")
            raise CkitValidationException(MSG_0019)

    def __call__(self, instance):
        self.validate(instance)


if __name__ == "__main__":
    print(report_schema())
