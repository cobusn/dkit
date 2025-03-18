#!/usr/bin/env python
# Copyright (c) 2025 Cobus Nel
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all
# copies or substantial portions of the Software.abs#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
"""
Print the structure of a json file

Useful to extract keys for JQ type queries
"""

import json
import argparse
__version__ = "25.3.1"


def list_json_keys(data, parent_key=""):
    """Recursively lists keys in a JSON object.

    Args:
        data: The JSON data (dictionary or list).
        parent_key: The key of the parent object (for nested structures).
    """
    if isinstance(data, dict):
        for key, value in data.items():
            new_key = parent_key + "." + key if parent_key else key
            print(new_key)  # Or append to a list if you want to return the keys
            list_json_keys(value, new_key)
    elif isinstance(data, list):
        for i, value in enumerate(data):
            new_key = parent_key + f"[{i}]" if parent_key else f"[{i}]"
            list_json_keys(value, new_key)


def main():
    description = __doc__
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('file_name', help='json file')
    args = parser.parse_args()
    with open(args.file_name) as infile:
        list_json_keys(json.load(infile))


if __name__ == "__main__":
	main()
