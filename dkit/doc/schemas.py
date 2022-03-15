#  shcema for report.yaml
import cerberus
import yaml
import logging

from ..exceptions import DKitValidationException
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
        styler:
            required: True
        builder:
            allowed:
                - reportlab
                - latex
        plot_folder:
            required: False
            type: string
        template_folder:
            required: True
            type: string
        version:
            required: True
            type: integer
latex:
    required: False
    type: list
    schema:
        type: string
styles:
    type: list
    schema:
        type: string
documents:
    type: list
    schema:
        type: string
presentations:
    type: list
    schema:
        type: string
dict_schema: &_dict
    type: dict
    keyschema:
        type: string
    valueschema:
        type: string
document:
    type: dict
    schema:
        author:
            type: string
        title:
            type: string
        sub_title:
            type: string
            nullable: True
        date:
            type: datetime
            nullable: True
        contact:
            type: string
            nullable: True
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
                err = error
            raise DKitValidationException(MSG_0019, str(err))

    def __call__(self, instance):
        self.validate(instance)


if __name__ == "__main__":
    print(report_schema())
