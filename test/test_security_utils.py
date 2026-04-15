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

'''
Created on 19 January 2016
'''
import sys
from pathlib import Path

TEST_DIR = Path(__file__).parent
OUTPUT_DIR = TEST_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

sys.path.insert(0, str(TEST_DIR.parent))  # noqa

from dkit.doc.lorem import Lorem  # noqa
import unittest
from cryptography.fernet import InvalidToken
from dkit.utilities.security import (  # noqa
    FernetBytes, Vigenere, Pie, Fernet, EncryptedStore, EncryptedIO,
    gen_password
)

# Shared key fixture — generated once per test session
TEST_KEY = FernetBytes.generate_key()


class TestVigenere(unittest.TestCase):
    """Test Vigenere simple obfuscation"""

    test_case = [
        "b;asdurjvb893498udkxcm,adk",
        "0002",
        "2309u82234502",
        "SAA2345s@##$%dASF",
        "asdafasdfas",
    ]

    @classmethod
    def setUpClass(cls):
        cls.key = "12345"
        cls.C = Vigenere

    def test_vigenere(self):
        "Test encrypt - decrypt"
        for m in self.test_case:
            o = self.C(self.key)
            e = o.encrypt(m)
            self.assertEqual(m, o.decrypt(e))

    def test_invalid_key_type(self):
        "Test raise on invalid type"
        with self.assertRaises(TypeError):
            self.C(0)

    def test_invalid_msg_type(self):
        "Test raise on invalid message type"
        with self.assertRaises(TypeError):
            o = self.C(self.key)
            o.encrypt(0)

    def test_invalid_encrypted_type(self):
        "Test raise on invalid encrypted type"
        with self.assertRaises(TypeError):
            o = self.C(self.key)
            o.decrypt(0)

    def test_zero_key_length(self):
        "Test for error with zero key length"
        with self.assertRaises(ValueError):
            self.C("")


class TestFernet(TestVigenere):
    """Test Fernet encryption"""

    @classmethod
    def setUpClass(cls):
        cls.C = Fernet
        cls.key = TEST_KEY

    def test_generate_key(self):
        key = Fernet.generate_key()
        self.assertIsInstance(key, str)
        self.assertGreater(len(key), 0)

    def test_wrong_key_fails(self):
        other_key = Fernet.generate_key()
        a = Fernet(self.key)
        b = Fernet(other_key)
        enc = a.encrypt("text")
        with self.assertRaises(InvalidToken):
            b.decrypt(enc)

    def test_from_password(self):
        salt = Fernet.generate_salt()
        a = Fernet.from_password("password", salt)
        b = Fernet.from_password("password", salt)
        enc = a.encrypt("text")
        dec = b.decrypt(enc)
        self.assertEqual("text", dec)

    def test_from_password_different_salt(self):
        a = Fernet.from_password("password", Fernet.generate_salt())
        b = Fernet.from_password("password", Fernet.generate_salt())
        enc = a.encrypt("text")
        with self.assertRaises(InvalidToken):
            b.decrypt(enc)

    def test_zero_key_length(self):
        "Test that an empty key is rejected before any encryption attempt"
        with self.assertRaises(ValueError):
            Fernet("")


class TestFernetBytes(unittest.TestCase):
    """Test FernetBytes (bytes-in / bytes-out) encryption"""

    @classmethod
    def setUpClass(cls):
        cls.fernet = FernetBytes(TEST_KEY)

    def test_encrypt_decrypt_roundtrip(self):
        data = b"hello bytes"
        enc = self.fernet.encrypt(data)
        self.assertEqual(self.fernet.decrypt(enc), data)

    def test_encrypt_returns_bytes(self):
        self.assertIsInstance(self.fernet.encrypt(b"data"), bytes)

    def test_decrypt_returns_bytes(self):
        enc = self.fernet.encrypt(b"data")
        self.assertIsInstance(self.fernet.decrypt(enc), bytes)

    def test_wrong_key_fails(self):
        other = FernetBytes(FernetBytes.generate_key())
        enc = self.fernet.encrypt(b"secret")
        with self.assertRaises(InvalidToken):
            other.decrypt(enc)

    def test_invalid_msg_type(self):
        with self.assertRaises(TypeError):
            self.fernet.encrypt("string not bytes")


class TestPie(unittest.TestCase):

    def test_pie(self):
        text = "as;dlfjasld;asdf@@"
        o = Pie()
        e = o.encrypt(text)
        self.assertEqual(text, o.decrypt(e))


class TestEncStore(unittest.TestCase):
    """encrypted store"""

    @classmethod
    def setUpClass(cls):
        store_path = OUTPUT_DIR / "test_enc_store.json"
        store_path.unlink(missing_ok=True)
        cls.store = EncryptedStore.from_json_file(
            TEST_KEY, str(store_path)
        )
        # Pre-populate known keys for iter/len tests
        cls.store["int"] = 1
        cls.store["float"] = 1.0
        cls.store["str"] = "string"

    @classmethod
    def tearDownClass(cls):
        if cls.store is not None:
            cls.store.close()
            del cls.store

    def test_set_int(self):
        self.assertEqual(self.store["int"], 1)

    def test_set_float(self):
        self.assertEqual(self.store["float"], 1.0)

    def test_set_str(self):
        self.assertEqual(self.store["str"], "string")

    def test_del(self):
        self.store["temp"] = 1
        del self.store["temp"]
        self.assertNotIn("temp", self.store)

    def test_len(self):
        self.assertEqual(len(self.store), 3)

    def test_iter_keys(self):
        self.assertEqual(set(self.store.keys()), {"int", "float", "str"})

    def test_context_manager(self):
        store_path = OUTPUT_DIR / "test_enc_store_ctx.json"
        store_path.unlink(missing_ok=True)
        with EncryptedStore.from_json_file(TEST_KEY, str(store_path)) as store:
            store["key"] = "value"
            self.assertEqual(store["key"], "value")

    def test_invalid_encryptor(self):
        with self.assertRaises(TypeError):
            EncryptedStore(backend={}, encryptor="not_an_encryptor")


class TestEncryptedIO(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        lorem = Lorem()
        cls.fernet = FernetBytes(TEST_KEY)
        cls.data = lorem.txt_paragraph(max=10)
        cls.fname = OUTPUT_DIR / "encrypted_data.txt"
        EncryptedIO(cls.fernet).write(cls.fname, cls.data.encode())

    def test_write(self):
        self.assertTrue(self.fname.exists())

    def test_read(self):
        text = EncryptedIO(self.fernet).read(self.fname)
        self.assertEqual(text.decode(), self.data)

    def test_tampered_file_raises(self):
        tampered = OUTPUT_DIR / "tampered.txt"
        tampered.write_bytes(b"this is not a valid fernet token")
        with self.assertRaises(InvalidToken):
            EncryptedIO(self.fernet).read(tampered)


class TestFunctions(unittest.TestCase):

    ALLOWED_CHARS = set(
        '23456qwertasdfgzxcvbQWERTASDFGZXCVB'
        '789yuiophjknmYUIPHJKLNM'
        '!@#$%^&*'
    )

    def test_gen_password_len(self):
        pwd = gen_password(20)
        self.assertEqual(len(pwd), 20)

    def test_gen_password_chars(self):
        pwd = gen_password(100)
        self.assertTrue(set(pwd).issubset(self.ALLOWED_CHARS))


if __name__ == '__main__':
    unittest.main()
