import sys; sys.path.insert(0, "..")  # noqa
import unittest

import numpy as np
from scipy.sparse import rand

from dkit.utilities.numeric import (
    serialize_array,
    deserialize_array,
    hash_array,
)


N = 200


def load_matrix(n=N):
    return np.random.rand(n, n)


def load_sparse_matrix(n=N):
    return rand(n, n, format='csr')


class TestNumericUtilities(unittest.TestCase):

    def setUp(self):
        self.data = load_matrix()
        self.target = load_matrix()

    def test_hash_ndarray(self):
        """test hash to identify ndarray"""
        copy = self.data[:]
        self.assertEqual(
            hash_array(copy),
            hash_array(self.data)
        )

    def test_hash_csr_matrix(self):
        """test hash to identify csr_matrix"""
        data = load_sparse_matrix()
        copy = data[:]
        self.assertEqual(
            hash_array(copy),
            hash_array(data)
        )

    def test_serde_ndarray(self):
        """test de/serialize ndarray"""
        f = serialize_array(self.data)
        data = deserialize_array(f)
        self.assertEqual(
            type(self.data),
            type(data)
        )
        self.assertEqual(
            hash_array(self.data),
            hash_array(data)
        )

    def test_serde_csr_array(self):
        """test de/serialize csr_array"""
        csr = load_sparse_matrix()
        f = serialize_array(csr, kind="csr_matrix")
        serde_csr = deserialize_array(f, kind="csr_matrix")
        self.assertEqual(
            type(csr),
            type(serde_csr)
        )
        self.assertEqual(
            hash_array(csr),
            hash_array(serde_csr)
        )


if __name__ == '__main__':
    unittest.main()
