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
"""
Messagepack utilities including Encoder/Decoder for specialized
datatypes.

This was helpful information:
http://stackoverflow.com/questions/30313243/messagepack-and-datetime

"""
import datetime
import dateutil
import gc
import decimal

_DATETIME_EXT_TYPE = 42
_DATE_EXT_TYPE = 43
_DECIMAL_TYPE = 44


class MsgpackEncoder(object):
    """
    Encoder/Decoder for msgpack

    42: datetime.datetime
    43: datetime.date
    44: decimal
    """
    msgpack = __import__("msgpack")

    def _unpacker_hook(self, code, data):
        if code == _DATETIME_EXT_TYPE:
            values = self.unpack(data)
            if len(values) == 8:  # we have timezone
                return datetime.datetime(*values[:-1], dateutil.tz.tzoffset(None, values[-1]))
            else:
                return datetime.datetime(*values)
        elif code == _DATE_EXT_TYPE:
            values = self.unpack(data)
            return datetime.date(*values)
        elif code == _DECIMAL_TYPE:
            return decimal.Decimal(self.unpack(data))
        return self.msgpack.ExtType(code, data)

    # This will only get called for unknown types
    def _packer_unknown_handler(self, obj):
        if isinstance(obj, datetime.datetime):
            if obj.tzinfo:
                components = (
                    obj.year, obj.month, obj.day, obj.hour, obj.minute, obj.second,
                    obj.microsecond, int(obj.utcoffset().total_seconds())
                )
            else:
                components = (
                    obj.year, obj.month, obj.day, obj.hour, obj.minute, obj.second,
                    obj.microsecond
                )
            # we effectively double pack the values to "compress" them
            data = self.msgpack.ExtType(_DATETIME_EXT_TYPE, self.pack(components))
            return data
        elif isinstance(obj, datetime.date):
            components = (obj.year, obj.month, obj.day)
            return self.msgpack.ExtType(_DATE_EXT_TYPE, self.pack(components))
        elif isinstance(obj, decimal.Decimal):
            data = str(obj)
            return self.msgpack.ExtType(_DECIMAL_TYPE, self.pack(data))
        raise TypeError("Unknown type: {}, {}".format(str(type(obj)), obj))

    def pack(self, obj, **kwargs):
        # we don't use a global packer because it wouldn't be re-entrant safe
        return self.msgpack.packb(
            obj, use_bin_type=True, default=self._packer_unknown_handler, **kwargs
        )

    def unpack(self, payload, raw=False):
        try:
            # we temporarily disable gc during unpack to bump up
            #  performance: https://pypi.python.org/pypi/msgpack-python
            gc.disable()

            # This must match the above _packer parameters above.  NOTE: use_list is faster
            return self.msgpack.unpackb(
                payload, use_list=False, raw=raw, ext_hook=self._unpacker_hook
            )
        finally:
            gc.enable()

    def unpacker(self, file_like, raw=False):
        return self.msgpack.Unpacker(
            file_like,
            read_size=1024*1024,
            use_list=False,
            raw=raw,
            ext_hook=self._unpacker_hook
        )
