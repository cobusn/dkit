import sys; sys.path.insert(0, "..")  # noqa
import unittest

import numpy as np
from scipy.sparse import rand
from dkit.utilities.concurrency import (
    SHMLoader,
    SHMSaver,
)
from dkit.utilities.numeric import hash_array
from zlib import adler32

N = 200


def load_matrix(n=N):
    return np.random.rand(n, n)


def load_sparse_matrix(n=N):
    return rand(n, n, format='csr')


class TestSharedMemoryManager(unittest.TestCase):

    def test_shared_ndarray(self):
        """Test shared ndarray"""
        data = load_matrix()
        target = load_matrix()
        saver = SHMSaver(data=data, target=target)
        loader = SHMLoader(saver.properties)
        shm_data = loader.get("data")
        self.assertTrue(np.alltrue(shm_data == data))
        saver.close()

    def test_shared_csr_matrix(self):
        """Test shared csr_matrix"""
        csr_data = load_sparse_matrix()
        saver = SHMSaver(csr_data=csr_data)
        loader = SHMLoader(saver.properties)
        shm_data = loader.get("csr_data")
        self.assertEqual(
            type(csr_data),
            type(shm_data)
        )
        self.assertEqual(
            hash_array(csr_data),
            hash_array(shm_data)
        )
        saver.close()


if __name__ == '__main__':
    unittest.main()
