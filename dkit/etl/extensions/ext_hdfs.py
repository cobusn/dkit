# Copyright (c) 2018 Cobus Nel
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
Provide Reader and Writer objects for HDFS

Utilizes the python HdfsCLI library internally
"""


from .. import writer, reader

# hdfs imported in class to save startup times


class HDFSWriter(writer.ClosedWriter):
    """
    HDFS Writer

    Implements hdfs.InsecureClient

    resource_uri examples are `http://localhost:14000` for httpfs or `http://localhost:50070`

    Args:
        * resource_uri: uri to hdfs or httpfs resouce
        * user: hadoop user
        * path: file path
        * encoding: default to utf-8. Useful for json
        * overwrite: overwrite existing file
        * append: append to existing file when True
    """
    def __init__(self, path, user, resource_uri, encoding='utf-8', overwrite=False,
                 append=False, compression=None):
        self.uri = resource_uri
        self.user = user
        self.path = path
        self.append = append
        self.overwrite = overwrite
        self.encoding = encoding
        self.compression = compression

    def open(self):
        from hdfs import InsecureClient
        if self.compression:
            encoding = None
        else:
            encoding = "utf-8"
        self.client = InsecureClient(self.uri, user=self.user)
        _client = self.client.write(
            self.path,
            encoding=encoding,
            overwrite=self.overwrite,
            append=self.append
        )
        if self.compression:
            return self.compression.open(
                _client,
                "wt"
            )
        else:
            return _client


class HDFSReader(reader.ClosedReader):
    """
    HDFS Writer

    Implements hdfs.InsecureClient

    resource_uri examples are `http://localhost:14000` for httpfs or `http://localhost:50070`

    Args:
        * resource_uri: uri to hdfs or httpfs resouce
        * user: hadoop user
        * path: file path
        * encoding: default to utf-8. Useful for json
    """
    def __init__(self, resource_uri, user, path, encoding='utf-8'):
        self.uri = resource_uri
        self.user = user
        self.path = path
        self.encoding = encoding

    def open(self):
        """instantiate file like object"""
        from hdfs import InsecureClient
        self.client = InsecureClient(self.uri, user=self.user)
        return self.client.read(
            self.path,
            encoding=self.encoding
        )
