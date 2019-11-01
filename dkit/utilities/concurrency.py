# Copyright (c) 2019 Cobus Nel
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
"""
Utilities to assist with concurrent programming:
"""
from multiprocessing import shared_memory
import numpy as np
from scipy.sparse import csr_matrix
from .numeric import hash_array


class AbstractDataManager(object):

    def __del__(self):
        if hasattr(self, "shm_map"):
            self.close()


class SHMLoader(AbstractDataManager):

    def __init__(self, properties):
        self.properties = properties
        self._instances = []
        self.__serde_map = {
            "ndarray": self._get_ndarray,
            "csr_matrix": self._get_csr_matrix,
        }

    def close(self):
        for shm in self._instances:
            shm.close()

    def _get_csr_matrix(self, properties):
        data = self._get_ndarray(properties["data"])
        indices = self._get_ndarray(properties["indices"])
        indptr = self._get_ndarray(properties["indptr"])
        c = csr_matrix((data, indices, indptr))
        return c

    def _get_ndarray(self, properties):
        name = properties["name"]
        shm = shared_memory.SharedMemory(name=name)
        self._instances.append(shm)
        c = np.ndarray(
            properties["shape"],
            properties["dtype"],
            buffer=shm.buf
        )
        return c

    def get(self, name):
        """instanticate instance from shared memory"""
        props = self.properties[name]
        kind = props["class"]
        return self.__serde_map[kind](props)


class SHMSaver(AbstractDataManager):
    """Save data to Shared Memory

    Args:
        - `**kwargs`: data to persist

    """

    def __init__(self, **kwargs):
        self.data_map = kwargs   # original_data
        self.__hash_map = {}     # mapped to hash
        self.properties = {}
        self._shm_map = {}
        self.serde_map = {
            "ndarray": self._ndarray_saver,
            "csr_matrix": self._csr_matrix_saver,
        }
        for k, data in self.data_map.items():
            hash_ = str(hash_array(data))
            self.__hash_map[hash_] = data
            saver = self.serde_map[type(data).__name__]
            properties = saver(
                data,
                hash_=hash_
            )
            self.properties[k] = properties

    @property
    def hash_map(self):
        """Look up array using hash"""
        return self.__hash_map

    def close(self):
        """close shared memory"""
        for shm in self._shm_map.values():
            shm.close()

    def unlink(self):
        """close shared memory"""
        for shm in self._shm_map.values():
            shm.unlink()

    def _ndarray_saver(self, data, hash_):
        """Save data to shared memory"""
        shm = shared_memory.SharedMemory(name=hash_, create=True, size=data.nbytes)
        loader = np.ndarray(data.shape, dtype=data.dtype, buffer=shm.buf)
        loader[:] = data[:]
        properties = {
            "name": shm.name,
            "shape": data.shape,
            "dtype": data.dtype.str,
            "class": type(data).__name__,
        }
        self._shm_map[hash_] = shm
        return properties

    def _csr_matrix_saver(self, data, hash_):
        """Save csr_matrix to shared memory"""
        properties = {
            "name": hash_,
            "shape": data.shape,
            "dtype": data.dtype.str,
            "class": type(data).__name__,
        }
        components = ("data", "indices", "indptr")
        for component in components:
            c = getattr(data, component)
            h = str(hash_array(c))
            properties[component] = self._ndarray_saver(c, h)
        return properties
