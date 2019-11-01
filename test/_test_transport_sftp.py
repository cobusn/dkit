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

import unittest
import sys
sys.path.insert(0, "..")
from dkit.etl.extensions.ext_sftp import SFTPPasswordTransport


class TestSFTPPasswordTransport(unittest.TestCase):
    """Test Vigenere simple obfuscation"""

    def test_transport(self):
        t = SFTPPasswordTransport(
            host="<<hostname>>",
            port=22,
            user="<<user>>",
            password="<<password>>",
        )
        t.connect()
        print(t.list_dir("192883/Invoices/XML/"))
        t.get_files(["192883/Invoices/XML/7162749_530764.xml"], ".")
        t.close()


if __name__ == '__main__':
    unittest.main()
