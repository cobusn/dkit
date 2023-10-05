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
"""
Provide simple obfuscation
"""
from __future__ import annotations
import base64
import importlib
import math
import pickle
import shelve
import configparser
import os
from typing import Type, TypeVar
from abc import ABC, abstractmethod
from collections.abc import MutableMapping
from ..data.containers import JSONShelve
from .. import GLOBAL_CONFIG_FILE, LOCAL_CONFIG_FILE
from pathlib import Path
# ALPHA = 'abcdefghijklmnopqrstuvwxyz'


__all__ = [
    "Fernet",
    "FernetBytes",
    "EncryptedStore",
    "Vigenere",
    "Pie"
]


AE = TypeVar("AE", bound="AbstractEncryptor")


class AbstractEncryptor(ABC):

    valid_types = [str, bytes]

    def __init__(self, the_key):
        self.__key = None   # for property, value is set below
        self.str_valid_types = ", ".join([str(i) for i in self.valid_types])
        self.keylen = 0  # set by property
        if the_key is None:
            self.key = base64.urlsafe_b64encode((2 * str(math.pi))[:32].encode()).decode()
        else:
            self.key = the_key

    def __get_key(self):
        """
        Encryption key

        :raise:
            :TypeError: Key is of invalid type
            :ValueError: Key is of length zero
        """
        return self.__key

    def __set_key(self, value):
        # Type Check
        self._validate(value, self.valid_types)

        if len(value) == 0:
            raise ValueError("[key] must be of length >0")
        self.__key = value
        self.keylen = len(value)

    def _validate(self, obj, valid_types):
        """
        Validate that obj is an instance of at least one of the types in valid_types
        """
        if not any([isinstance(obj, i) for i in valid_types]):
            raise TypeError(
                "[msg] should be an instance of one of the following: %s"
                % self.str_valid_types
            )

    key = property(__get_key, __set_key)

    @abstractmethod
    def encrypt(self, msg: str) -> str:
        pass

    @abstractmethod
    def decrypt(self, msg: str) -> str:
        pass

    @staticmethod
    def generate_key():
        _fernet = importlib.import_module("cryptography.fernet")
        return _fernet.Fernet.generate_key().decode("utf-8")

    @classmethod
    def from_config(
        cls: Type[AE], config_file=None, key=None, null=False
    ) -> AE:
        """
        instantiate an instance from configuration provided

        arguments:
            - config_file: instantiate from configuration file
            - secret: instantiate from secret provided
            - null: null configuration
        """
        global_path = os.path.expanduser(GLOBAL_CONFIG_FILE)
        default_local_path = os.path.expanduser(LOCAL_CONFIG_FILE)

        if isinstance(config_file, (str, Path)):
            # filename specified
            config_files = [config_file]
        else:
            if os.path.exists(default_local_path):
                # Nothing specified, attempt to load defaults
                config_files = [default_local_path]
            elif os.path.exists(global_path):
                config_files = [global_path]

        if null:
            _key = None
        elif key is not None:
            _key = key
        else:
            if len(config_files) == 0:
                raise Exception("No valid configuration files found")
            c = configparser.ConfigParser()
            c.read(set(config_files))
            _key = c.get("DEFAULT", "key")
        return cls(_key)


class FernetBytes(AbstractEncryptor):
    """
    Helper class wrapping Fernet encryption

    See:
        - https://cryptography.io/en/latest/fernet/
        - https://asecuritysite.com/encryption/fernet
    """
    valid_types = [bytes, str]

    def __init__(self, the_key: str):
        super().__init__(the_key)
        self._fernet = importlib.import_module("cryptography.fernet")

    def encrypt(self, msg: str) -> str:
        self._validate(msg, self.valid_types)
        return self._fernet.Fernet(self.key).encrypt(msg)

    def decrypt(self, msg):
        """use stored key to encrypt a string"""
        self._validate(msg, self.valid_types)
        return self._fernet.Fernet(self.key).decrypt(msg)


class Fernet(FernetBytes):
    """
    Helper class wrapping Fernet encryption

    See:
        - https://cryptography.io/en/latest/fernet/
        - https://asecuritysite.com/encryption/fernet
    """
    valid_types = [str]

    def encrypt(self, msg: str) -> str:
        self._validate(msg, self.valid_types)
        return self._fernet.Fernet(self.key).encrypt(msg.encode()).decode()

    def decrypt(self, msg):
        """use stored key to encrypt a string"""
        self._validate(msg, self.valid_types)
        return self._fernet.Fernet(self.key).decrypt(msg.encode()).decode()


class Vigenere(AbstractEncryptor):
    """
    Implementation of Vigenere cipher for simple obfuscation

    This class is an implementation of the Vigenere cipher that will
    perform simple obfuscation / de-obfuscation.  Applicable use cases
    is to obfuscate passwords from a casual browser.

    Details can be found here wikipedia:
    https://en.wikipedia.org/wiki/Vigen%C3%A8re_cipher

    and this stack overflow question:
    http://stackoverflow.com/questions/5131227/custom-python-encryption-algorithm
    """
    def encrypt(self, msg: str):
        """
        Encrypt message

        Args:
            - msg: message to encrypt

        Returns:
            encrypted message

        Raises:
            msg of invalid type
        """
        self._validate(msg, self.valid_types)

        encrypted = []
        key = self.key
        for i, c in enumerate(msg):
            key_c = ord(key[i % self.keylen])
            msg_c = ord(c)
            encrypted.append(chr((msg_c + key_c) % 127))
        encr_str = ''.join(encrypted).encode("ascii")
        return base64.urlsafe_b64encode(encr_str)

    def decrypt(self, encrypted):
        """
        Decrypt message

        Args:
            - encrypted: message to encrypt

        Returns:
            decrypted message

        Raises:
            - TypeError: encrypted is of invalid type
        """
        if isinstance(encrypted, str):
            encrypted = encrypted.encode()
        msg = []
        for i, c in enumerate(base64.urlsafe_b64decode(encrypted)):
            key_c = ord(self.key[i % self.keylen])
            enc_c = c
            msg.append((enc_c - key_c) % 127)

        return ''.join([chr(i) for i in msg])


class Pie(Vigenere):

    def __init__(self):
        super().__init__(str(math.pi))


ES = TypeVar("ES", bound="EncryptedStore")


class EncryptedStore(MutableMapping):
    """
    Encrypted Store

    Arguments:
        - backend: backend instance
        - encryptor: encryptor instance
    """
    def __init__(self, backend=None, encryptor=None):
        self.be: shelve = backend
        self.enc: AbstractEncryptor = self.__validate_encryptor(encryptor)

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def __validate_encryptor(self, encryptor):
        if isinstance(encryptor, AbstractEncryptor):
            return encryptor
        else:
            raise TypeError("Invalid Encryptor instance")

    def __getitem__(self, key):
        ser = self.enc.decrypt(self.be[key])
        return pickle.loads(ser)

    def __setitem__(self, key, value):
        ser = pickle.dumps(value)
        self.be[key] = self.enc.encrypt(ser)

    def __delitem__(self, key):
        del self.be[key]

    def __iter__(self):
        yield from self.be.keys()

    def __len__(self):
        return len(self.be)

    def close(self):
        if self.be and hasattr(self.be, "close"):
            self.be.close()

    @classmethod
    def from_key(cls: Type[ES], key: str, backend) -> ES:
        """
        instantiate a new instance

        Arguments:
            - secret: secret used to instantiate
            - backend: backend instance (e.g. shelve)

        Example:

            from dkit.data.containers import JSONShelve

            with JSONShelve.open("test.db") as be:
                with EncryptedStore.from_key("secret", be) as db:
                    db["key"] = "my secret"

        """
        fernet = FernetBytes(key)
        return cls(encryptor=fernet, backend=backend)

    @classmethod
    def from_json_file(cls: Type[ES], key: str, file_name: str) -> ES:
        """
        convenience function to open a json based
        store

        Arguments:
            secret: secret key
            file_name: filename of json file
        """
        with JSONShelve(file_name) as json_backend:
            return cls.from_key(key, json_backend)
