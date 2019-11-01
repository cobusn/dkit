"""
Utilities to encode and decode to (B)ib(T)ex (R)ecord (bxr) format. It is a data
format inspired by the Bibtex format.
"""
from functools import singledispatch
from datetime import datetime, date, time
from ..parsers import record_parser


class _BTRParser(record_parser.AbstractRecordParser):

    def __init__(self, lines):
        rules = [
            (r"\s*@(\w*){(\w*)", self.start_record),
            (r'\s*(\w+)\s*=\s*"([^"]+)"', self.process_str),
            (r"\s*(\w+)\s*=\s*'([^']+)'", self.process_str),
            (r"\s*(\w+)\s*=\s*(-?\d+)\s*$", self.process_int),
            (r"\s*(\w+)\s*=\s*(-?\d+\.\d+)", self.process_float),
            (r"^\s*(\w+)\s*=\s*(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})",
             self.process_datetime),
            (r"\s*(\w+)\s*=\s*(\d{4})-(\d{2})-(\d{2})", self.process_date),
            (r"\s*(\w+)\s*=\s*(true|false)", self.process_bool),
            (r"\s*(\w+)\s*=\s*(\d{2}):(\d{2}):(\d{2})", self.process_time),
            (r"\s*}", self.end_record),
        ]
        super().__init__(lines, rules)


def dumps_mapping(the_dict):
    """
    dump dictionary to string bxr format

    :param the_dict: dictionary object to encode
    :returns: string object
    """
    return "".join(
        "".join(
            _iter_encode(v, key=k)
        ) for k, v in the_dict.items()
    )


def dump_mapping(the_dict, fp):
    """
    dump dict of dicts to file object

    :param the_dict: dictionary object
    :param fp: file object
    """
    for key, value in the_dict.items():
        fp.write(
            "".join(_iter_encode(value, key=key))
        )


def dump(record, fp):
    """
    dump dict to file object

    """
    fp.write(
        "".join(_iter_encode(record))
    )


def dumps_iter(the_iterable):
    """
    dump iterable to string

    :param the_iterable: iterable of dict objects
    :returns: string object
    """
    return "".join("".join(_iter_encode(r)) for r in the_iterable)


def dump_iter(the_iterable, fp):
    """
    dump iterable to file

    :param the_iterable: iterable of dict objects
    """
    for row in the_iterable:
        fp.write("".join(_iter_encode(row)))


def load(fp):
    """
    load dictionary from file

    :param fp: file pointer
    :returns: dict
    """
    loader = _BTRParser(fp)
    retval = {}
    for row in loader:
        retval[loader.last_key] = row
    return retval


def loads_iter(the_string):
    """
    generator for loading dict objects from string

    :param the_string: string object with encoding
    :returns: iterator of dict objects
    """
    yield from _BTRParser(the_string.splitlines())


def load_iter(fp):
    """
    generator to load records iteravely from file

    :param fp: file object
    :returns: iterator of dict objects
    """
    yield from _BTRParser(fp)


def loads(the_string):
    """
    load dictionary of records from string object

    :param the_string: string object
    :returns: dict
    """
    loader = _BTRParser(the_string.splitlines())
    retval = {}
    for row in loader:
        retval[loader.last_key] = row
    return retval


def _iter_encode(record, key="", record_type=""):
    """
    encode to bxr format
    """

    @singledispatch
    def _encode(value):
        return '"' + value + '"'

    @_encode.register(int)
    @_encode.register(float)
    def _(value):
        return value

    @_encode.register(time)
    def _reg_time(value):
        return value.strftime("%H:%M:%S")

    @_encode.register(date)
    def _reg_date(value):
        return value.strftime("%Y-%m-%d")

    @_encode.register(datetime)
    def _reg_datetime(value):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    @_encode.register(bool)
    def _reg_bool(value):
        if value:
            return "true"
        else:
            return "false"

    yield "@{}{{{}\n".format(record_type, key)
    for key, value in record.items():
        yield "\t{} = {}\n".format(key, _encode(value))
    yield "}\n"
