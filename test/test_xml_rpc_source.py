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
from __future__ import print_function
import sys
import unittest
import threading
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
sys.path.insert(0, "..")

from dkit.etl.source import XmlRpcSource


def get_list():
    return [1, 2, 3]


def adder(l, r):
    return l + r


def get_iterable():
    return [[1], [2], [3]]


class TestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        class RequestHandler(SimpleXMLRPCRequestHandler):
            rpc_paths = ('/RPC2',)
        cls.server = SimpleXMLRPCServer(("localhost", 8001), requestHandler=RequestHandler)
        cls.server.register_function(get_list, "get_list")
        cls.server.register_function(adder, "add")
        cls.server.register_function(get_iterable, "get_iterable")
        cls.xml_server_thread = threading.Thread(target=cls.server.serve_forever).start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        super(TestCase, cls).tearDownClass()

    def setUp(self):
        self.o = XmlRpcSource("http://localhost:8001", "get_iterable", [])

    def test_1(self):
        """
        test l_call
        """
        r = self.o._call("get_list", [])
        self.assertEqual(r, [1, 2, 3])

    def test_2(self):
        """
        test proxy
        """
        r = self.o.proxy.add(1, 1)
        self.assertEqual(r, 2)

    def test_iterable(self):
        """
        test retrieving from iterable
        """
        ll = list(self.o)
        self.assertEqual(len(ll), 3, "Ensure length of 3")


if __name__ == '__main__':
    unittest.main()
