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
Encryption and obfuscation utilities.

Provides authenticated encryption via Fernet (AES-128-CBC + HMAC-SHA256),
simple obfuscation via the Vigenere cipher, and encrypted key-value store
and file I/O helpers.
"""
from __future__ import annotations
import base64
import importlib
import math
import pickle
import shelve
import configparser
import os
from typing import Self, Optional
from abc import ABC, abstractmethod
from collections.abc import MutableMapping
from ..data.containers import JSONShelve
from .. import GLOBAL_CONFIG_FILE, LOCAL_CONFIG_FILE
from pathlib import Path
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import secrets
# ALPHA = 'abcdefghijklmnopqrstuvwxyz'


__all__ = [
    "Fernet",
    "FernetBytes",
    "EncryptedStore",
    "EncryptedIO",
    "Vigenere",
    "Pie"
]


def gen_password(length: int = 10) -> str:
    '''
    Generate random passwords
    '''
    allchars = (
        '23456qwertasdfgzxcvbQWERTASDFGZXCVB'
        '789yuiophjknmYUIPHJKLNM'
        '!@#$%^&*'
    )
    return ''.join(secrets.choice(allchars) for _ in range(length))


class AbstractEncryptor(ABC):
    """Abstract base class for encryptors.

    Subclasses must implement ``encrypt`` and ``decrypt``. The ``key``
    property validates and stores the encryption key on assignment.
    """

    valid_types = [str, bytes]

    def __init__(self, the_key: str | bytes):
        self.__key = None   # for property, value is set below
        self.str_valid_types = ", ".join([str(i) for i in self.valid_types])
        self.keylen = 0  # set by property
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

    def _validate(self, obj: object, valid_types: list[type]):
        """
        Validate that obj is an instance of at least one of the types in valid_types
        """
        if not any(isinstance(obj, i) for i in valid_types):
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
    def generate_key() -> str:
        """Generate a new random Fernet-compatible key.

        Returns:
            A URL-safe base64-encoded 32-byte key as a string.
        """
        _fernet = importlib.import_module("cryptography.fernet")
        return _fernet.Fernet.generate_key().decode("utf-8")

    @staticmethod
    def generate_salt() -> bytes:
        """Generate a random 16-byte salt for use with ``from_password``.

        Returns:
            16 cryptographically random bytes.
        """
        return secrets.token_bytes(16)

    @classmethod
    def from_password(cls, password: str, salt: bytes) -> Self:
        """Derive a Fernet-compatible key from a plain text password.

        The same ``password`` and ``salt`` combination always produces the
        same key, so the salt must be stored alongside any encrypted data
        to allow future decryption. Use ``generate_salt()`` to create a
        suitable salt.

        Args:
            password: plain text password.
            salt: 16-byte salt. Use ``generate_salt()`` to create one.

        Returns:
            A new encryptor instance initialised with the derived key.
        """
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=600_000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
        return cls(key.decode())

    @classmethod
    def from_config(cls, config_file: Optional[str | Path] = None) -> Self:
        """Instantiate an encryptor from a configuration file.

        Reads the ``key`` field from the ``[DEFAULT]`` section. When
        ``config_file`` is not provided the local config is tried first,
        falling back to the global config.

        Args:
            config_file: path to a config file. Uses default search paths
                when not provided.

        Returns:
            A new encryptor instance initialised with the key from config.

        Raises:
            FileNotFoundError: when no valid configuration file is found.
        """
        global_path = os.path.expanduser(GLOBAL_CONFIG_FILE)
        default_local_path = os.path.expanduser(LOCAL_CONFIG_FILE)
        config_files = []

        if isinstance(config_file, (str, Path)):
            # filename specified
            config_files = [config_file]
        else:
            # Nothing specified, attempt to load defaults
            if os.path.exists(default_local_path):
                config_files = [default_local_path]
            elif os.path.exists(global_path):
                config_files = [global_path]

        if len(config_files) == 0:
            raise FileNotFoundError("No valid configuration files found")

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

    def encrypt(self, msg: bytes) -> bytes:
        """Encrypt bytes using the stored Fernet key.

        Args:
            msg: plaintext bytes to encrypt.

        Returns:
            Fernet token as bytes.
        """
        self._validate(msg, self.valid_types)
        return self._fernet.Fernet(self.key).encrypt(msg)

    def decrypt(self, msg: bytes) -> bytes:
        """Decrypt a Fernet token using the stored key.

        Args:
            msg: Fernet token bytes to decrypt.

        Returns:
            Decrypted plaintext bytes.
        """
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
        """Encrypt a string using the stored Fernet key.

        Args:
            msg: plaintext string to encrypt.

        Returns:
            Fernet token as a string.
        """
        self._validate(msg, self.valid_types)
        return self._fernet.Fernet(self.key).encrypt(msg.encode()).decode()

    def decrypt(self, msg: str) -> str:
        """Decrypt a Fernet token string using the stored key.

        Args:
            msg: Fernet token string to decrypt.

        Returns:
            Decrypted plaintext string.
        """
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
    def encrypt(self, msg: str) -> bytes:
        """Encrypt a message using the Vigenere cipher.

        Args:
            msg: plaintext string to encrypt.

        Returns:
            URL-safe base64-encoded ciphertext as bytes.

        Raises:
            TypeError: if msg is not a valid type.
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

    def decrypt(self, encrypted: str | bytes) -> str:
        """Decrypt a Vigenere-ciphered message.

        Args:
            encrypted: URL-safe base64-encoded ciphertext as str or bytes.

        Returns:
            Decrypted plaintext string.

        Raises:
            TypeError: if encrypted is not a valid type.
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
    """Vigenere cipher pre-keyed with the digits of pi.

    Convenience subclass for lightweight obfuscation where no key
    management is required. Not suitable for sensitive data.
    """

    def __init__(self):
        super().__init__(str(math.pi))


class EncryptedStore(MutableMapping):
    """
    Encrypted Shelve type Store

    Arguments:
        - backend: backend instance
        - encryptor: encryptor instance
    """
    def __init__(self, backend: MutableMapping = None, encryptor: AbstractEncryptor = None):
        self.be: shelve.Shelf = backend
        self.enc: AbstractEncryptor = self.__validate_encryptor(encryptor)

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
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
    def from_key(cls, key: str, backend: MutableMapping) -> Self:
        """Instantiate an EncryptedStore from a Fernet key and a backend.

        Args:
            key: Fernet-compatible encryption key string.
            backend: a MutableMapping backend (e.g. shelve, JSONShelve).

        Returns:
            A new EncryptedStore instance.

        Example::

            from dkit.data.containers import JSONShelve

            with JSONShelve.open("test.db") as be:
                with EncryptedStore.from_key("secret", be) as db:
                    db["key"] = "my secret"
        """
        fernet = FernetBytes(key)
        return cls(encryptor=fernet, backend=backend)

    @classmethod
    def from_json_file(cls, key: str, file_name: str) -> Self:
        """Instantiate an EncryptedStore backed by a JSON file.

        Args:
            key: Fernet-compatible encryption key string.
            file_name: path to the JSON store file.

        Returns:
            A new EncryptedStore instance. Use as a context manager to
            ensure the backend is closed on exit.
        """
        json_backend = JSONShelve(file_name)
        return cls.from_key(key, json_backend)


class EncryptedIO:
    """Read and write Fernet-encrypted binary files.

    Args:
        fernet: a FernetBytes encryptor instance used for all I/O operations.
    """

    def __init__(self, fernet: FernetBytes):
        self._fernet = fernet

    def read(self, file_name: str | Path) -> bytes:
        """Read and decrypt an encrypted file.

        Args:
            file_name: path to the encrypted file.

        Returns:
            Decrypted file contents as bytes.
        """
        with open(file_name, "rb") as infile:
            return self._fernet.decrypt(
                infile.read()
            )

    def write(self, file_name: str | Path, data: bytes):
        """Encrypt and write data to a file.

        Args:
            file_name: path to the output file.
            data: plaintext bytes to encrypt and write.
        """
        with open(file_name, "wb") as outfile:
            outfile.write(
                self._fernet.encrypt(data)
            )
