"""
Simple Object Database and serialisation
"""
import json
import os
import shutil
import tempfile
from abc import ABC
import yaml
import dataclasses

from .. import messages
from .containers import DictionaryEmulator


class Object(ABC):

    def as_dict(self):
        """
        return a dict version of object
        """
        return dataclasses.asdict(self)

    def on_set(self, db_instance):
        """
        trigger that is called each time a value is set
        """
        pass


class ObjectMap(DictionaryEmulator):
    """
    Simple object mapper that map objects to a key value. The objects are stored as dicitonary
    values and serialized on access.  This design implies the following constraints:

    .. warning::

        For managed types, each such type must have an `as_dict()` method and it must be
        instantiable with a dict of its values.

    Args:
        kind: the type. this type must have an as_dict() method.

    Raises:
        TypeError, ValueError

    Objects are serialized each time they are accessed and stored as dicts internally.
    """
    def __init__(self, kind: type, container: "EntityMapDB" = None, meta: dict = None):
        if not hasattr(kind, "as_dict"):
            raise AttributeError(messages.MSG_0010.format(kind.__name__, "as_dict"))
        if not hasattr(kind, "on_set"):
            raise AttributeError(messages.MSG_0010.format(kind.__name__, "on_set"))
        self.kind = kind
        self.store = dict()
        self.container = container
        self.meta = dict()

    def __setitem__(self, key, value):
        # Type check
        if not isinstance(value, self.kind):
            required = self.kind.__name__
            got = value.__class__.__name__
            raise ValueError(messages.MSG_0011.format(required, got))

        # run validations
        value.on_set(self.container)
        self.store[key] = value.as_dict()

    def __getitem__(self, key):
        return self.kind(**self.store[key])

    def from_dict(self, the_dict: dict):
        """
        read data from provided dictionary

        :param the_dict: dictionary object
        :returns self:
        """
        self.meta = the_dict.pop("__meta__", {})
        for key, value in the_dict.items():
            self[key] = self.kind(**value)
        return self

    def as_dict(self) -> dict:
        """
        return map as dictionary
        """
        return self.store


class ObjectMapDB(object):
    """
    Container class for object maps that can serialize the data.

    :schema dict: entity name mapped to type

    """
    def __init__(self, schema, **kwargs):
        self.schema = schema
        self.meta = {}
        self.__objects = {}
        self.__init_schema()
        super().__init__(**kwargs)

    def __getattr__(self, name):
        return self.__objects[name]

    def as_dict(self) -> dict:
        """
        serialize database to map
        """
        retval = {"__meta__": self.meta}
        for key, value in self.__objects.items():
            retval[key] = value.as_dict()
        return retval

    def from_dict(self, the_dict: dict):
        """
        load from dictionary

        :param the_dict: dictionary instance
        :returns self:
        """
        self.meta = the_dict.get("__meta__", {})
        for item, collectn in self.__objects.items():
            collectn.from_dict(the_dict[item])
        return self

    def __init_schema(self):
        for key, kind in self.schema.items():
            if key == "__meta__":
                self.meta = kind
            else:
                self.__objects[key] = ObjectMap(kind, self)

    def _empty_schema(self):
        """
        blank schema
        """
        return {k: {} for k in self.__objects.keys()}


class FileLoaderMixin(object):
    """
    mixin to provide save and load methods for serialization to file.

    :param codec: codec (e.g. yaml, json, pickle)
    :param backups: create backup when writing (default is true)
    :param binary: set to true for binary codecs such as pickle
    :param save_options: dictionary to pass to codec

    .. note::

        the class will create a blank file when loading if the specified
        filename does not exist
    """
    def __init__(self, codec=json, backups=True, binary=False, **kwargs):
        self.codec = codec
        self.binary = binary
        self.backups = backups
        super().__init__(**kwargs)

    def save(self, filename, codec=None):
        """
        write schema to file

        create backup of previous file
        first write to temp file then copy over original file
        optionally use a different codec
        """
        if codec is None:
            codec = self.codec
        the_model = self.as_dict()
        mode = "wb" if self.binary else "w"
        if codec.__name__ == "yaml":
            save_options = {"default_flow_style": False}
        else:
            save_options = {}
        if codec.__name__ in ['pickle']:
            if "b" not in mode:
                mode += "b"
        try:
            fd, name = tempfile.mkstemp(text=True)
            file_obj = os.fdopen(fd, mode)
            codec.dump(the_model, file_obj, **save_options)
        finally:
            file_obj.close()
            if self.backups and os.path.exists(filename):
                shutil.copy(filename, "{}.bak".format(filename))
            shutil.copy(name, filename)
            os.remove(name)
        return self

    def load(self, filename):
        """
        read schema or create if none

        .. note::

            the class will create a blank file when loading if the specified
            filename does not exist

        """
        mode = "rb" if self.binary else "r"
        # Ensure binary mode for picle files
        if self.codec.__name__ in ["pickle"]:
            if "b" not in mode:
                mode += "b"
        if os.path.exists(filename):
            with open(filename, mode) as infile:
                if self.codec.__name__ == "yaml":
                    data = self.codec.load(infile, Loader=yaml.SafeLoader)
                else:
                    data = self.codec.load(infile)
                self.from_dict(data)
        else:
            self.from_dict(self._empty_schema())
        return self


class FileObjectMapDB(FileLoaderMixin, ObjectMapDB):
    """
    Container class for entity maps that can serialize the data
    to file (using pickle/yamle/jaon syntax).

    :schema dict: object name mapped to type

    .. include:: ../../examples/example_file_object_map_db.py
        :literal:

    Produce:

    .. include:: ../../examples/example_file_object_map_db.out
        :literal:

    .. note::

        Specify binary=True for binary serialization such as
        pickle.
    """
    pass
