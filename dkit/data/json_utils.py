# Code adapted from: http://broadcast.oreilly.com/2009/05/pymotw-json.html

from datetime import datetime, date
import json
import decimal
import importlib


class CustomCodec(object):

    def __init__(self, obj_type):
        self.obj_type = obj_type

    @property
    def name(self):
        return self.obj_type.__name__

    def encode(self, obj):
        raise NotImplementedError

    def decode(self, obj):
        raise NotImplementedError


class NPInt64Codec(CustomCodec):

    def __init__(self):
        self.np = importlib.import_module("numpy")
        super().__init__(self.np.int64)

    def encode(self, obj):
        """
        encode datetime object to dictionary
        """
        return int(obj)


class NPFloat64Codec(CustomCodec):

    def __init__(self):
        self.np = importlib.import_module("numpy")
        super().__init__(self.np.float64)

    def encode(self, obj):
        """
        encode datetime object to dictionary
        """
        return float(obj)


def numpy_codecs():
    codecs = [NPFloat64Codec, NPInt64Codec]
    return [i() for i in codecs]


class DateTimeCodec(CustomCodec):
    """
    Serialize datetime.datetime to unix timestamp
    """

    def __init__(self):
        super().__init__(datetime)

    def encode(self, obj):
        """
        encode datetime object to dictionary
        """
        return {
            '__type__': self.name,
            'timestamp': obj.timestamp()
        }

    def decode(self, obj):
        """
        decode datetime from dictionary format
        """
        return datetime.fromtimestamp(obj['timestamp'])


class Decimal2FloatCodec(CustomCodec):

    def __init__(self):
        super().__init__(decimal.Decimal)

    def encode(self, obj):
        return float(obj)


class DateStrCodec(CustomCodec):

    def __init__(self):
        super().__init__(date)

    def encode(self, obj):
        return str(obj)


class DateTimeStrCodec(CustomCodec):

    def __init__(self):
        super().__init__(datetime)

    def encode(self, obj):
        return str(obj)


class DateCodec(CustomCodec):
    """
    Serialize datetime.datetime to unix timestamp
    """

    def __init__(self):
        super().__init__(date)

    def encode(self, obj):
        """
        encode datetime object to dictionary
        """
        return {
            '__type__': self.name,
            'timestamp': int(datetime(obj.year, obj.month, obj.day).timestamp())
        }

    def decode(self, obj):
        """
        decode datetime from dictionary format
        """
        return datetime.fromtimestamp(obj['timestamp']).date()


class DateTimeDictCodec(CustomCodec):

    def __init__(self):
        super().__init__(datetime)

    def encode(self, obj):
        """
        encode datetime object to dictionary
        """
        return {
            '__type__': self.name,
            'year': obj.year,
            'month': obj.month,
            'day': obj.day,
            'hour': obj.hour,
            'minute': obj.minute,
            'second': obj.second,
            'microsecond': obj.microsecond,
        }

    def decode(self, obj):
        """
        decode datetime from dictionary format
        """
        return datetime(**obj)


class DateDictCodec(CustomCodec):

    def __init__(self):
        super().__init__(date)

    def encode(self, obj):
        """
        encode datetime object to dictionary
        """
        return {
            '__type__': self.name,
            'year': obj.year,
            'month': obj.month,
            'day': obj.day,
        }

    def decode(self, obj):
        """
        decode datetime from dictionary format
        """
        return date(**obj)


class JsonSerializer(object):
    """
    Serialize and de-serialize custom classes to and
    from json

    :param plugins: list of plugins
    """
    def __init__(self, *codecs, encoder=json):
        self.encoder = encoder
        self.__codecs = {}
        self.i = 0
        for codec in codecs:
            self.add_codec(codec)

    def add_codec(self, codec):
        self.__codecs[codec.name] = codec

    def to_json(self, obj):
        """
        serialize class instance to json.

        :param obj: object
        :returns: encoded object
        """
        try:
            class_name = obj.__class__.__name__
            return self.__codecs[class_name].encode(obj)
        except Exception:
            self.encoder.JSONEncoder.default(self, obj)

    def from_json(self, obj):
        """
        de-serialize json object to class instance
        """
        try:
            the_type = obj.pop('__type__')
        except Exception:
            return obj
        return self.__codecs[the_type].decode(obj)

    def dump(self, obj, fp, **kwargs):
        """
        convenience function that call json.dump
        """
        return self.encoder.dump(obj, fp, allow_nan=False, default=self.to_json, **kwargs)

    def dumps(self, obj, **kwargs):
        """
        convenience function that call json.dumps
        """
        return self.encoder.dumps(obj, allow_nan=False, default=self.to_json, **kwargs)

    def load(self, fp, **kwargs):
        """
        convenience function that call json.load
        """
        return self.encoder.load(fp, object_hook=self.from_json, **kwargs)

    def loads(self, obj, **kwargs):
        """
        convenience function that calls json.loads
        """
        return self.encoder.loads(obj, object_hook=self.from_json, **kwargs)
