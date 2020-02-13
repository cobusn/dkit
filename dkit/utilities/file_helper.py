# Copyright (c) 2019 Cobus Nel

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
import tempfile
import pathlib
import yaml
from typing import Union, TextIO, Text
import re

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

__all__ = [
    "yaml_load",
    "sanitise_name",
]


def yaml_load(stream: Union[TextIO, Text]):
    """
    helper function parse yaml from text

    Args:
        - stream: file like object

    """
    return yaml.load(stream, Loader=Loader)


def temp_filename(root=None, suffix=None) -> pathlib.Path:
    """
    generate temporary filename

    arguments:
        root: root folder. use system default if none (/tmp)
        suffix: file suffix (optional)

    returns: <pathlib.Path>  object
    """
    _root = root if root else tempfile._get_default_tempdir()
    _fname = next(tempfile._get_candidate_names())
    if suffix is not None:
        _fname = _fname + "." + suffix
    retval = pathlib.Path(_root) / _fname
    return retval


def sanitise_name(file_name):
    """
    sanitize text to be suitable as filenames:

        -   change to lower case
        -   replace spaces with underscore
    """
    s = re.sub(r"[^\w\s]", '', file_name.strip().lower())
    s = re.sub(r"\s+", '-', s)
    return s
