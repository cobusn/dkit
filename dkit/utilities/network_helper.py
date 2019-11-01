# Copyright (C) 2015  Cobus Nel
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
Network Helpers.
"""
from urllib.parse import urlparse
from os.path import basename
from requests.auth import HTTPBasicAuth
import requests


def download_file(url, local_filename=None, verify=True, username=None, password=None,
                  binary_mode=False):
    """
    Convenience function to download a file from provided URL.

    Note that this routine will throw generic exceptions if any problem is encountered
    with file handling.


    .. include:: ../../examples/example_download_file.py
       :literal:

    Will produce the following output:

    .. include:: ../../examples/example_download_file.out
       :literal:

    :param str url: URL
    :param str local_filename: Local filename. If not provided, determine from URL.
    :param bool verify: Verify certificate details
    :param str username: Username when authentication is required. Leave as none if not.
    :param str password: Password when authentication is required.  Leave as not if not.
    :param bool binary_mode: Set to True if file mode binary is required.
    :retval int: status reported by http
    :retval str: reason provided for status
    :retval str: filename
    """
    def get_filename():
        """get local file name"""
        if not local_filename:
            d_url = urlparse(url)
            return basename(d_url.path)
        else:
            return local_filename

    def get_auth():
        """Get HTTPBasicAuth object"""
        if username and password:
            return HTTPBasicAuth(username, password)
        else:
            return None

    def get_file_mode():
        if binary_mode:
            return "wb"
        else:
            return "w"

    r = requests.get(url, auth=get_auth(), verify=verify)
    filename = get_filename()
    if r.status_code == 200:
        # 200 OK
        with open(filename, get_file_mode()) as lf:
            for chunk in r:
                lf.write(chunk)

    return r.status_code, r.reason, filename
