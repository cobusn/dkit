# Copyright (c) 2017 Cobus Nel
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

import re

from .. import exceptions, messages


COMPRESSION_FORMATS = ['bz2', 'zip', 'gz', 'xz', 'lz4', "snappy"]
ENCRYPTION_FORMATS = ['aes']
RE_COMRESSION_FORMATS = "|".join(COMPRESSION_FORMATS)
RE_ENCRYPTION_FORMATS = "|".join(ENCRYPTION_FORMATS)
FILE_DIALECTS = [
    'csv', 'jsonl', 'json', 'tsv', 'xlsx', 'xls', 'xml', 'bxr', 'pkl', 'mpak', 'pke'
]
SHARED_MEMORY_DIALECTS = ["shm"]
FILE_DB_DIALECTS = ["hdf5", "sqlite"]
NETWORK_DIALECTS = ["mysql", "oracle", "mssql", "postgres", "impala"]
SQL_DRIVERS = {
    "firebird": "firebird+fdb",
    "hdf5": "hdf5",
    "impala": "impala",
    "mssql": "mssql+pymssql",
    "mysql": "mysql+mysqldb",
    "oracle": "oracle+cx_oracle",
    "postgres": "postgres",
    "sqlite": "sqlite",
}


class URIStruct:
    """
    Helper class that ensure parsed dictionary contains the correct
    fields.
    """
    def __init__(self, dialect=None, driver=None, database=None, username=None, password=None,
                 host=None, port=None, compression=None, entity=None, filter=None):
        # encryption=None):
        self.dialect = dialect
        self.driver = driver
        self.database = database
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.compression = compression
        self.entity = entity
        # self.encryption = encryption
        self.filter = filter

    @classmethod
    def from_uri(cls, uri):
        """parse from uri"""
        return cls(**parse(uri))

    def as_dict(self):
        """return as dict"""
        return {k: v for k, v in self.__dict__.items() if not k.startswith("__")}

    def __str__(self):
        """string representation"""
        return str(self.as_dict())

    def __eq__(self, other):
        """equality test"""
        return self.__dict__ == other.__dict__


def parse(uri):
    """
    parse uri into dictionary.

    Arguments:
        uri: uri string (e.g 'jsonl://filename.db')

    Returns:
        dictionary (see URIStruct)

    Raises
       `dkit.exceptions.CkitParseException`
    """
    if ":///" in uri:
        # this is a file based driver
        retval = _parse_file_driver(uri)
    elif "//" in uri:
        # this is a network based database
        retval = _parse_network_db(uri)
    else:
        # this is a file
        retval = _parse_file_name(uri)
    if retval is not None:
        return retval
    else:
        raise exceptions.CkitParseException(
            messages.MSG_0012.format(uri)
        )


def _parse_file_driver(uri):
    """parse file with specified driver"""
    rx = r"({}):\/\/\/(.+)$".format("|".join(
        FILE_DIALECTS + FILE_DB_DIALECTS + SHARED_MEMORY_DIALECTS
    ))
    m = re.match(rx, uri)
    if m is not None:
        dialect = m.group(1)
        if dialect in FILE_DB_DIALECTS:
            # Its a database
            endpoint = _parse_file_db_endpoint(m.group(2))
            if endpoint is not None:
                endpoint["dialect"] = dialect
                endpoint["driver"] = SQL_DRIVERS[dialect]
                return URIStruct(**endpoint).as_dict()
            else:
                return None
        elif dialect in SHARED_MEMORY_DIALECTS:
            # It is a shared memory file
            return URIStruct(
                database=f"/{m.group(2)}",
                dialect=_parse_dialect_from_filename(m.group(2)),
                driver="shm",
                compression=_parse_compression_from_filename(m.group(2)),
                # encryption=_parse_encryption_from_filename(m.group(2))
            ).as_dict()
        elif dialect in FILE_DIALECTS:
            return URIStruct(
                database=m.group(2),
                dialect=m.group(1),
                driver="file",
                compression=_parse_compression_from_filename(m.group(2)),
                # encryption=_parse_encryption_from_filename(m.group(2))
            ).as_dict()
        else:
            raise exceptions.CkitParseException(messages.MSG_0014.format(dialect))
    else:
        return None


def _parse_file_db_endpoint(host_string):
    """parse host details including port etc."""
    # user:password@hostname:port/database::entity[filter]
    rx = (
        r"(?P<database>[a-zA-Z0-9_./]+)"                 # file
        r"(?:\?(?P<entity>[a-zA-Z0-9/_-]+)(?:#\[(?P<filter>.+)\])?)?"  # entity
        r"$"                                             # end of rx
    )
    m = re.match(rx, host_string)
    if m is not None:
        return m.groupdict()
    else:
        return None


def _parse_network_db(host_string):
    """parse host details including port etc."""
    # user:password@hostname:port/database?entity[filter]
    rx = (
        r"(?P<dialect>{}):\/\/".format("|".join(NETWORK_DIALECTS)) +
        r"(?:(?P<username>.+):(?P<password>.*)@)?"       # username / password
        r"(?P<host>[a-zA-Z0-9_.-]+)"                      # host
        r"(?::(?P<port>[0-9]+))?"                        # port
        r"(?:\/(?P<database>[-.\w]+))?"                      # database
        r"(?:\?(?P<entity>[\w_]+)(?:#\[(?P<filter>.+)\])?)?"  # entity
        r"$"                                             # end of rx
    )
    m = re.match(rx, host_string)
    if m is not None:
        retval = URIStruct(**m.groupdict()).as_dict()
        retval["driver"] = SQL_DRIVERS[retval["dialect"]]
        return retval
    else:
        return None


def _parse_file_name(uri):
    """parse filename to uri"""
    return URIStruct(
        database=uri,
        compression=_parse_compression_from_filename(uri),
        # encryption=_parse_encryption_from_filename(uri),
        driver="file",
        dialect=_parse_dialect_from_filename(uri)
    ).as_dict()


def _parse_dialect_from_filename(file_name):
    """
    determine encoding from filename
    """
    p = re.compile(r".+\.({})(?:\..+$)?".format("|".join(FILE_DIALECTS)))
    r = p.search(file_name)
    if r is None:
        raise(exceptions.CkitParseException(messages.MSG_0013.format(file_name)))
    else:
        return r.group(1)


def _parse_compression_from_filename(file_name):
    """
    determine compression from filename
    """
    p = re.compile(r".+\.({})(?:\..+$)*$".format(RE_COMRESSION_FORMATS))
    r = p.search(file_name)
    if r is None:
        return None
    else:
        return r.group(1)


# def _parse_encryption_from_filename(file_name):
#    """
#    determine encryption from filename
#    """
#    p = re.compile(r".+\.({})$".format(RE_ENCRYPTION_FORMATS))
#    r = p.search(file_name)
#    if r is None:
#        return None
#    else:
#        return r.group(1)
