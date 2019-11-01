from collections import namedtuple
from tdigest._tdigest import lib as _lib
DEFAULT_COMPRESSION = 400

Centroid = namedtuple("Centroid", ("weight", "mean"))

# Adapted from https://github.com/kpdemetriou/tdigest-cffi


class TDigest:
    """
    Tdigest implementation

    TDigest implementation adapted from https://github.com/kpdemetriou/tdigest-cffi

    args:
        compression
    """
    def __init__(self, compression=DEFAULT_COMPRESSION):
        if not isinstance(compression, int):
            raise TypeError("'compression' must be of type 'int'")

        if compression <= 0:
            raise ValueError("'compression' must larger than 0")

        self._struct = _lib.tdigest_new(compression)

    def __del__(self):
        if hasattr(self, "_struct"):
            _lib.tdigest_free(self._struct)

    def _compress(self):
        _lib.tdigest_compress(self._struct)

    @property
    def compression(self): return self._struct.compression

    @property
    def threshold(self):
        return self._struct.threshold

    @property
    def size(self):
        return self._struct.size

    @property
    def weight(self):
        if self._struct.point_count:
            self._compress()

        return self._struct.weight

    @property
    def centroid_count(self):
        if self._struct.point_count:
            self._compress()

        return self._struct.centroid_count

    @property
    def compression_count(self):
        return self._struct.compression_count

    def insert(self, value, weight=1):
        """insert new value"""
        if not isinstance(value, (float, int)):
            raise TypeError("'value' must be of type 'float' or 'int'")

        if not isinstance(weight, int):
            raise TypeError("'weight' must be of type 'int'")

        if weight <= 0:
            raise ValueError("'weight' must larger than 0")

        _lib.tdigest_add(self._struct, value, weight)

    def median(self):
        return self.quantile(0.5)

    def iqr(self):
        return self.quantile(0.75) - self.quantile(0.25)

    def quantile(self, value):
        """quantile at value"""
        if not isinstance(value, float):
            raise TypeError("'value' must be of type 'float'")

        if value < 0.0 or value > 1.0:
            raise ValueError("'value' must be between 0.00 and 1.00")

        return _lib.tdigest_quantile(self._struct, value)

    def percentile(self, value):
        """percentile at value"""
        if not isinstance(value, (int, float)):
            raise TypeError("'value' must be of type 'float' or 'int'")

        if value < 0 or value > 100:
            raise ValueError("'value' must be between 0 and 100")

        return _lib.tdigest_quantile(self._struct, value / 100)

    def cdf(self, value):
        """cumulative distribution function"""
        if not isinstance(value, (int, float)):
            raise TypeError("'value' must be of type 'float' or 'int'")

        return _lib.tdigest_cdf(self._struct, value)

    def centroids(self):
        """yields Centoids"""
        for i in range(self.centroid_count):
            centroid = self._struct.centroids[i]
            yield Centroid(centroid.weight, centroid.mean)

    def merge(self, other):
        if not isinstance(other, (TDigest, )):
            raise TypeError("'value' must be of type 'TDigest' or 'RawTDigest'")

        _lib.tdigest_merge(self._struct, other._struct)
