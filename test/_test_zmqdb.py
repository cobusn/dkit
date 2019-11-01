import sys
sys.path.insert(0, "..")
from dkit.data.zmqdb import Connection
from zmqdb_server import ports
import time
import unittest
import tabulate


class TestZMQDB(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.conn = Connection("ipc://33321")
        cls.client = cls.conn.client("numbers")

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def test_lookup(self):
        """test lookup"""
        for i in range(1000):
            self.assertEqual(self.client[i], i)

    def test_in(self):
        """test in statement"""
        for i in range(1000):
            self.assertEqual(i in self.client, True)

    def test_keys(self):
        self.assertEqual(
            list(self.client.keys()),
            list(range(1000))
        )

    def test_performance(self):
        results = []
        for port in ports:
            print(port)
            conn = Connection(port)
            c = conn.client("numbers")
            start = time.perf_counter()
            for _ in range(10):
                for i in range(1000):
                    n = c[i]
            duration = time.perf_counter() - start
            results.append(
                {
                    "protocol": port,
                    "time": duration,
                    "performance": 10000.0 / duration
                }
            )
        print(tabulate.tabulate(results, headers="keys"))


if __name__ == '__main__':
    unittest.main()
