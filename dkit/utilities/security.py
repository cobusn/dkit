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
import math
import base64
from abc import ABC, abstractmethod
import importlib

ALPHA = 'abcdefghijklmnopqrstuvwxyz'


class AbstractEncryptor(ABC):

    valid_types = [str]

    def __init__(self, the_key):
        self.__key = None
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

    def _validate(self, obj, valid_types):
        """
        Validate that obj is an instance of at least one of the types in valid_types
        """
        if not any([isinstance(obj, i) for i in valid_types]):
            raise TypeError("[msg] should be an instance of one of the following: %s"
                            % self.str_valid_types)

    key = property(__get_key, __set_key)

    @abstractmethod
    def encrypt(self, msg: str) -> str:
        pass

    @abstractmethod
    def decrypt(self, msg: str) -> str:
        pass


class Fernet(AbstractEncryptor):
    """
    Helper class wrapping Fernet encryption

    See:
        - https://cryptography.io/en/latest/fernet/
        - https://asecuritysite.com/encryption/fernet
    """
    def __init__(self, the_key: str):
        super().__init__(the_key)
        self._fernet = importlib.import_module("cryptography.fernet")

    def encrypt(self, msg: str) -> str:
        self._validate(msg, self.valid_types)
        return self._fernet.Fernet(self.key).encrypt(msg.encode()).decode()

    def decrypt(self, msg):
        """use stored key to encrypt a string"""
        self._validate(msg, self.valid_types)
        return self._fernet.Fernet(self.key).decrypt(msg.encode()).decode()

    @staticmethod
    def generate_key():
        _fernet = importlib.import_module("cryptography.fernet")
        return _fernet.Fernet.generate_key().decode("utf-8")


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
