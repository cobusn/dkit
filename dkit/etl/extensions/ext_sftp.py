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
import paramiko
from os import path
import logging


logger = logging.getLogger(__name__)


class Transport():
    """
    Base class for transport objects
    """
    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.transport = None

    def connect():
        pass

    def get(self, *kwds):
        pass

    def put(self, *kwds):
        pass

    def close(self):
        pass


class SFTPPasswordTransport(Transport):
    """
    Secure ftp transport with username, password
    """
    def connect(self):
        logger.info("Connecting to host: {}".format(self.host))
        self.transport = paramiko.Transport((self.host, self.port))
        self.transport.connect(username=self.user, password=self.password)
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)
        logger.info("Successfully connected to: {}:{}".format(self.host, self.port))

    def get_files(self, remote_files, local_path):
        """
        fetch files
        """
        for remote_name in remote_files:
            local_name = self.local_name(remote_name, local_path)
            self.get_file(remote_name, local_name)

    def get_file(self, remote_file, local_name):
        self.sftp.get(remote_file, local_name)
        logger.info("retrieving: {}".format(remote_file))

    def list_dir(self, remote_path):
        """
        get remote listing
        """
        return self.sftp.listdir(remote_path)

    def local_name(self, remote_name, local_path):
        """
        get local filename
        """
        return path.join(
            local_path,
            path.basename(remote_name)
        )

    def close(self):
        self.sftp.close()
        self.transport.close()
