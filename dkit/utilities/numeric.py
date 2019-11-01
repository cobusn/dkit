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
Utilities for working with numeric object (e.g numpy, scipy and pandas)
"""
import numpy as np
from scipy.sparse import csr_matrix
from io import BytesIO
from zlib import adler32


def hash_array(obj):
    """hash to identify an ndarray or csr_matrix"""
    if isinstance(obj, np.ndarray):
        return adler32(obj.tobytes())
    elif isinstance(obj, csr_matrix):
        return adler32(
            obj.data.tobytes() + obj.indices.tobytes() + obj.indptr.tobytes()
        )


def serialize_array(array, kind: str = "ndarray"):
    """Serialize ndarray or csr_matrix to bytes (compressed zip format)

    Args:
        - array: numpy type array
        - kind: "ndarray" or "csr_matrix"

    Returns: bytes
    """

    def serialize_ndarray(obj):
        """serialize an ndarray object"""
        f = BytesIO()
        np.savez_compressed(f, obj)
        f.seek(0)
        return f

    def serialize_csr_array(obj):
        f = BytesIO()
        np.savez_compressed(
            f,
            data=obj.data,
            indices=obj.indices,
            indptr=obj.indptr
        )
        f.seek(0)
        return f

    serde_map = {
        "ndarray": serialize_ndarray,
        "csr_matrix": serialize_csr_array,
    }
    return serde_map[kind](array).read()


def deserialize_array(byte_string, kind="ndarray"):
    """De-serizize ndarry or csr_matrix from bytes (compressed zip)

    Args:
        - byte_string:  bytes
        - kind: 'ndarray' or 'csr_matrix'

    Returns:
        - ndarray or csr_matrix
    """
    def deserialize_ndarray(data):
        f = BytesIO()
        f.write(data)
        f.seek(0)
        data = np.load(f, allow_pickle=True)
        return data['arr_0']

    def deserialize_csr_matrix(data):
        f = BytesIO()
        f.write(data)
        f.seek(0)
        data = np.load(f, allow_pickle=True)
        return csr_matrix(
            (
                data["data"],
                data["indices"],
                data["indptr"]
            )
        )

    serde_map = {
        "ndarray": deserialize_ndarray,
        "csr_matrix": deserialize_csr_matrix,
    }
    return serde_map[kind](byte_string)
