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
Classes that model etl artifacts:

=========== =============== =================================================
DATE        NAME            COMMENT
=========== =============== =================================================
Dec 2017    Cobus Nel       Created
Jan 2018    Cobus Nel       Updated
Jun 2018    Cobus Nel       Finalized initial version
Jan 2019    Cobus Nel       Added Relation class
Jan 2019    Cobus Nel       Added ModelServices class
Aug 2019    Cobus Nel       Refactor Services classes
=========== =============== =================================================
"""
import configparser
import importlib
import os
import re
from contextlib import contextmanager
from dataclasses import dataclass, asdict
from typing import List, Dict

import jinja2
from jinja2 import meta

import yaml
import pprint
from pathlib import Path
from . import schema, source, transform
from .. import exceptions, messages
from ..data import map_db, containers
from ..parsers import type_parser, uri_parser
from ..utilities import template_helper, security


CONFIG_SECTION = "DEFAULT"
DEFAULT_MODEL_FILE = "model.yml"
GLOBAL_CONFIG_FILE = "~/.dk.ini"
LOCAL_CONFIG_FILE = "dk.ini"


class Entity(containers.DictionaryEmulator):
    """
    Serialise Cerberus based entity model
    """
    @property
    def sorted_dict(self):
        pk = [
            k for k, v in self.as_entity_validator().schema.items()
            if "primary_key" in v and v["primary_key"] is True
        ]
        normal_fields = [i for i in self.keys() if i not in pk]
        sorted_keys = sorted(pk) + sorted(normal_fields)
        return {k: self[k] for k in sorted_keys}

    @classmethod
    def from_iterable(cls, iter_src: source.AbstractSource, p: float, k: int):
        """
        infer Entity schema from input iterable

        Args:
            src: source instance
            p: probability of using a record for measuring
            k: number of samples

        Returns:
            * the schema as a dict
        """
        cerberus_schema = schema.EntityValidator.dict_from_iterable(iter_src,  p=p, stop=k)
        return cls.from_cerberus(cerberus_schema)

    @classmethod
    def from_cerberus(cls, cerberus_dict):
        """
        constructor that create new shorthand instance from
        Cerberus formatted schema dictionary

        Arguments:
            cerberus_dictionary: Cerberus object

        Returns:
            Entity instance
        """
        retval = cls()
        retval.store = cls.encode(cerberus_dict)
        return retval

    def as_entity_validator(self):
        """
        Create SchemaValidator instance from self
        """
        d = self.as_dict()
        decoded = self.decode(d)
        return schema.EntityValidator(decoded)

    @staticmethod
    def decode(the_dict):
        """
        decode from short hand to dictionary format
        """
        parser = type_parser.TypeParser()
        return {k: parser.parse(v) for k, v in the_dict.items()}

    @classmethod
    def encode(cls, the_dict):
        """
        encode from dictionary to short hand format
        """
        retval = {}
        for k, v in the_dict.items():
            the_type = v["type"].capitalize()
            if the_type == "Datetime":
                the_type = "DateTime"
            retval[k] = "{}({})".format(
                the_type,
                cls.__encode_field(v)
            )
        return retval

    @staticmethod
    def __encode_field(the_dict):
        return ", ".join(
            ["{}={}".format(k, v) for k, v in the_dict.items() if k != "type"]
        )

    @staticmethod
    def get_listing(container):
        """obtain listing from container"""
        return [
            {
                "name": name,
                "# fields": len(schema),
            }
            for (name, schema)
            in container.items()
        ]

    def on_set(self, container):
        pass

    def __str__(self):
        return yaml.dump(
            self.as_dict(),
            default_flow_style=False
        )

    def __call__(self, the_iterable):
        """
        yield rows from input that is coerced to schema

        Args:
            iterable of dict

        Yields:
            rows coerced to schema
        """
        t = transform.CoerceTransform(self.as_entity_validator())
        yield from t(the_iterable)


@dataclass
class Connection(map_db.Object):
    dialect: str
    driver: str
    database: str
    username: str = None
    password: str = None
    host: str = None
    port: int = None
    compression: str = None
    encryption: str = None

    @staticmethod
    def get_listing(container):
        """obtain listing from container"""
        return [
            {
                "name": n,
                "dialect": uri.dialect,
                "database": uri.database
             }
            for (n, uri)
            in container.items()
        ]

    @classmethod
    def from_uri(cls, uri):
        """parse from uri"""
        uri_struct = uri_parser.parse(uri)
        del uri_struct["entity"]
        del uri_struct["filter"]
        return cls(**uri_struct)

    def as_uri(self):
        """return url formatted"""
        from .extensions.ext_sql_alchemy import URL
        return str(URL(**self.as_dict()))

    def as_dict(self, include_none=False):
        """to uri"""
        if not include_none:
            return {k: v for k, v in asdict(self).items() if v is not None}
        else:
            return asdict(self)


@dataclass
class Endpoint(map_db.Object):
    connection: str
    table_name: str = None
    entity: str = None

    @staticmethod
    def get_listing(container):
        """obtain listing from container"""
        return [
            {
                "name": n,
                "connection": e.connection,
                "table_name": e.table_name,
                "entity": e.entity,
             }
            for (n, e)
            in container.items()
        ]


@dataclass
class Relation(map_db.Object):
    constrained_entity: str
    constrained_columns: List[str]
    referred_entity: str
    referred_columns: List[str]

    @staticmethod
    def get_listing(container):
        """obtain listing from container"""
        return [
            {
                "name": n,
                "constrained_entity": r.constrained_entity,
                "referred_entity": r.referred_entity,
            } for
            (n, r) in container.items()
        ]


class Transform(containers.DictionaryEmulator):
    """
    Serialize Transform objects
    """
    pass

    def on_set(self, container):
        pass

    def __call__(self, the_iterable):
        """
        yield transformed rows from `the_iterable`
        """
        t = transform.FormulaTransform(self)
        yield from t(the_iterable)


@dataclass
class Query(map_db.Object):
    query: str
    description: str = ""

    def as_dict(self):
        return asdict(self)

    @staticmethod
    def get_listing(container):
        """obtain listing from container"""
        return [
            {
                "name": k,
                "description": v.description,
                "variables": len(v.variables),
            }
            for k, v in container.items()
        ]

    @property
    def template(self):
        """jinja2 instantiate template"""
        env = jinja2.Environment(
            undefined=jinja2.StrictUndefined,
            trim_blocks=True
        )
        return env.from_string(self.query.strip())

    @property
    def variables(self):
        """return set of template variables"""
        env = jinja2.Environment()
        ast = env.parse(self.query)
        return meta.find_undeclared_variables(ast)

    def __call__(self, **variables):
        """
        render query as a jinja template

        Args:
            vars: dictonary of variables

        Returns:
            rendered template
        """
        tpl = self.template
        template_data = template_helper.template_macros()
        template_data.update(variables)

        rendered = tpl.render(**template_data)
        return rendered


def get_model_codec(file_name):
    """
    get codec based on file_name extension
    """
    codec_map = {
        ".json": "json",
        ".yml": "yaml",
        ".pickle": "pickle",
    }
    _, ext = os.path.splitext(file_name)
    codec = importlib.import_module(codec_map[ext])
    return codec


def load_config(config):
    """
    load configurattion
    """
    if isinstance(config, configparser.ConfigParser):
        return config

    config_files = []
    global_path = os.path.expanduser(GLOBAL_CONFIG_FILE)
    if os.path.exists(global_path):
        config_files.append(global_path)

    # filename specified
    if isinstance(config, (str, Path)):
        config_files.append(config)
        c = configparser.ConfigParser()
        c.read(config_files)
        return c

    # Nothing specified, attempt to load defaults
    if config is None:
        c = configparser.ConfigParser()
        if os.path.exists(LOCAL_CONFIG_FILE):
            config_files.append(LOCAL_CONFIG_FILE)
        if len(config_files) == 0:
            raise exceptions.CkitConfigException(messages.MSG_0021)
        else:
            c.read([GLOBAL_CONFIG_FILE, LOCAL_CONFIG_FILE])
        return c


def load_model(cls, model_filename, config):
    if model_filename is None:
        model_filename = DEFAULT_MODEL_FILE

    return cls(
        config=load_config(config),
        codec=get_model_codec(model_filename)
    ).load(model_filename)


class ModelManager(map_db.FileObjectMapDB):
    """
    Model Database

    Attributes are:

        * connections
        * endpoints
        * queries
        * entities
        * transforms
        * relations

    """
    def __init__(self, config, codec=yaml):
        self.config = config
        super().__init__(
            schema={
                "__meta__": {
                    "version": "0.2"
                },
                "connections": Connection,
                "endpoints": Endpoint,
                "queries": Query,
                "entities": Entity,
                "transforms": Transform,
                "relations": Relation,
                },
            codec=codec
        )

    def __str__(self):
        pp = pprint.PrettyPrinter(indent=4)
        return pp.pformat(self.as_dict())

    @classmethod
    def from_file(cls, model_filename: str = None,
                  config: configparser.ConfigParser = None) -> "ModelManager":
        """
        file based constructor

        params:
            - model_filename: filename for model.yml file (assume model.yml)
            - config_instance: configuration file instance
        """
        return load_model(
            cls,
            model_filename,
            load_config(config),
        )

    @property
    def encryption_key(self):
        """encryption key in config"""
        return self.config.get("DEFAULT", "key")

    def add_connection(self, conn_name, uri, password=None):
        """save connection with encrypted password"""
        if conn_name in self.connections:
            raise exceptions.CkitApplicationException(
                "connection '{}' exists already".format(conn_name)
            )
        else:
            cryptor = security.Fernet(self.encryption_key)
            conn_instance = Connection.from_uri(uri)
            if password is not None:
                conn_instance.password = password
            if conn_instance.password is not None:
                conn_instance.password = cryptor.encrypt(conn_instance.password)
            self.connections[conn_name] = conn_instance
        return conn_instance

    def add_endpoint(self, name, connection: str, table: str, entity: str):
        """add and validate endpoint"""
        endpoint = Endpoint(
            connection,
            table,
            entity,
        )
        self.endpoints[name] = endpoint
        return endpoint

    def add_relation(self, name, const_entity, ref_entity, const_cols, ref_cols,
                     append=True):
        """
        add relation and perform validations

        args:
            - name: relation name
            - const_entity: constrained entity
            - ref_entity: referred entity
            - const_cols: constrained columns
            - ref_cols: referred columns

        returns:
            - relation object
        """
        # validations
        if name in self.relations:
            raise exceptions.CkitApplicationException(
                f"relation '{name}' exists already"
            )

        if len(const_cols) != len(ref_cols):
            raise exceptions.CkitApplicationException(
                "Relation should have an equal number of columns specified"
            )

        for column in ref_cols:
            if column not in self.entities[ref_entity]:
                raise exceptions.CkitApplicationException(
                    f"column '{column}' not in entity {ref_entity}"
                )

        for column in const_cols:
            if column not in self.entities[const_entity]:
                raise exceptions.CkitApplicationException(
                    f"column '{column}' not in entity {const_entity}"
                )

        new_rel = Relation(
            constrained_entity=const_entity,
            referred_entity=ref_entity,
            constrained_columns=const_cols,
            referred_columns=ref_cols
        )

        if append is True:
            self.relations[name] = new_rel

        return new_rel

    def get_connection(self, conn_name):
        """retrieve connection with encrypted password"""
        conn = self.connections[conn_name]
        if conn.password is not None:
            cryptor = security.Fernet(self.encryption_key)
            conn.password = cryptor.decrypt(conn.password)
        return conn

    def get_uri(self, uri):
        """
        build uri from entity and connection data

        Args:
            uri: uri to evaluate

        Returns:
            uri struct dictionary
        """
        if uri.startswith("::"):
            ep = self.endpoints[uri[2:]]
            conn_name = ep.connection
            uri_struct = self.connections[conn_name].as_dict(include_none=True)
            uri_struct["entity"] = ep.table_name
            uri_struct["filter"] = None
            if uri_struct["password"] is not None:
                encryptor = security.Fernet(self.encryption_key)
                uri_struct["password"] = encryptor.decrypt(
                    uri_struct["password"]
                )
            return uri_struct
        else:
            return uri_parser.parse(uri)

    @contextmanager
    def sink(self, uri, logger=None):
        """
        Instantiate a sink object from uri
        """
        # this need to be here due to recursive imports
        from . import utilities
        uri_struct = self.get_uri(uri)
        cleanup, factory = utilities._sink_factory(uri_struct, logger)
        try:
            yield factory
        finally:
            for obj in cleanup:
                obj.close()

    @contextmanager
    def source(self, uri: str, skip_lines: int = 0, field_names=None, logger=None, delimiter=","):
        """
        open context manager for source

        args:
            - uri: uri (use ::endpoint_name for endpoint)
            - skip_lines (number of lines to skip)
            - field names: list of fields to yield
            - logger
            - delimitier (",")

        returns:
            instantiated source
        """
        # this need to be here due to recursive imports
        from . import utilities
        try:
            parsed = self.get_uri(uri)
            factory = utilities._SourceIterFactory(
                parsed, skip_lines, field_names, logger, delimiter
            )
            yield factory
        finally:
            factory.close()

    def grep_entity_names(self, pattern, flags=0):
        """
        search entity names with regular expression parser

        yields:
            - names that match pattern
        """
        names = self.entities.keys()
        m = re.compile(pattern, flags=flags)
        return (i for i in names if m.match(i))

    def grep_fields(self, pattern, flags=0):
        """
        search field names with regular expression parser

        yields:
            - (entity, field_name) tuple
        """
        m = re.compile(pattern, flags=flags)
        for entity_name, entity in self.entities.items():
            for field_name in entity.keys():
                if m.match(field_name):
                    yield (entity_name, field_name)


class ETLServices(object):
    """
    implements model helper services

    Application logic is implemented in this class. The aim of this
    design is to separate the front end and application logic. For example,
    it should be possible to call this class from a web interface.

    Args:
        args:   argparse arguments
    """
    def __init__(self, model_instance, config_instance: configparser.ConfigParser):
        self.model = model_instance
        self.config = config_instance

    # constructors
    @classmethod
    def from_file(cls, model=None, config=None) -> "ETLServices":
        _config = load_config(config)
        return cls(
            load_model(ModelManager, model, _config),
            _config
        )

    @property
    def encryption_key(self):
        """encryption key in config"""
        return self.config.get("DEFAULT", "key")

    def _get_template(self, file_name: str):
        """
        instantiate jinja2 template from reference

        Args:
            template_ref: filename for template

        Returns:
            jinja2.Template instance
        """
        j2 = importlib.import_module("jinja2")
        with open(file_name) as template_file:
            return j2.Template(template_file.read())

    def render_template(self, template_ref: str, data_dict: Dict[str, str]) -> str:
        """
        return rendered template

        Args:
            template_ref: template name
            data_dict: dict of uri's

        Returns:
            rendered template
        """
        t = self._get_template(template_ref)
        instances = {k: self.input_stream_raw([v]) for k, v in data_dict.items()}
        return t.render(**instances)

    @staticmethod
    def init_config(config_uri):
        """
        initialize user configuration file

        * generate fernet key
        """
        fernet = importlib.import_module("cryptography.fernet")
        file_name = config_uri

        if os.path.exists(file_name):
            raise exceptions.CkitApplicationException(
                messages.MSG_0018.format(file_name)
            )
        else:
            config = configparser.ConfigParser()
            config[CONFIG_SECTION] = {
                "key": fernet.Fernet.generate_key().decode("utf-8"),
                "default_model_name": DEFAULT_MODEL_FILE,
            }
            with open(file_name, "w") as config_file:
                config.write(config_file)

    @staticmethod
    def init_model_file(model_uri: str):
        """
        @staticmethod initialize model file

        raises:
            - CkitApplicationException
        """
        if os.path.exists(model_uri):
            raise exceptions.CkitApplicationException(
                messages.MSG_0018.format(model_uri)
            )
        else:
            codec = get_model_codec(model_uri)
            new_model = ModelManager(None)            # None is ok for blank model
            new_model.save(model_uri, codec=codec)

    def __get_entity_relations(self, entity_names):
        """
        return filtered list of entity and relations
        """
        entities = {k: self.model.entities[k] for k in entity_names}
        relations = {
            k: v
            for k, v
            in self.model.relations.items()
            if (
                v.constrained_entity in entity_names
                and v.referred_entity in entity_names
            )
        }
        return entities, relations

    def export_model_entities(self, entity_names: List[str], output_file: str):
        """
        export entities with relations to a different schema.abs

        create schema if non existing
        """
        entities, relations = self.__get_entity_relations(entity_names)
        export_model = self.load_alternate_model(output_file)

        for entity in entities:
            export_model.entities[entity] = self.model.entities[entity]
        for relation in relations:
            export_model.relations[relation] = self.model.relations[relation]

        export_model.save(output_file)

    def export_schema(self, entity_names, kind, output_file, opts=None):
        """export schema

        args:
            - entity_names: list of entity names
            - kind: type of export
            - output_file: instantiated file

        """
        entities, relations = self.__get_entity_relations(entity_names)
        # export to graphviz
        if kind == "dot":
            exporter = importlib.import_module("dkit.etl.extensions.ext_graphviz")
            exported = exporter.create_erd(entities, relations)
        elif kind == "spark":
            # pyspark
            exporter = importlib.import_module("dkit.etl.extensions.ext_spark")
            spark_entities = {k: v.as_entity_validator() for k, v in entities.items()}
            exported = exporter.SchemaGenerator(**spark_entities).create_schema()
        elif kind == "sql.select":
            # sql via sqlalchemy
            exporter = importlib.import_module("dkit.etl.extensions.ext_sql_alchemy")
            sql_entities = {k: v.as_entity_validator() for k, v in entities.items()}
            exported = exporter.SQLAlchemyModelFactory().create_sql_select(
                opts["dialect"],
                **sql_entities
            )
        elif kind == "sql.create":
            # sql vie sqlalchemy
            exporter = importlib.import_module("dkit.etl.extensions.ext_sql_alchemy")
            sql_entities = {k: v.as_entity_validator() for k, v in entities.items()}
            exported = exporter.SQLAlchemyModelFactory().create_sql_schema(
                opts["dialect"],
                **sql_entities
            )
        else:
            exported = " An unknown Error occurred"

        # write to file
        output_file.write(exported)

    def load_alternate_model(self, model_filename: str):
        """
        load alternate model and create if non existing
        """
        if not os.path.exists(model_filename):
            export_model = ModelManager(codec=get_model_codec(model_filename))
        else:
            export_model = ModelManager(
                self.config,
                codec=get_model_codec(model_filename)
            ).load(model_filename)
        return export_model

    def save_model_file(self, model_db: ModelManager, file_name):
        """
        save model to disk
        """
        model_db.save(file_name, codec=get_model_codec(file_name))
