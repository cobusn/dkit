
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

import inspect
from dkit.utilities import introspection
import importlib
import argparse


class Inspecter(object):

    def run(elf, args):
        obj_path = args.object_name
        documenter = introspection.Documenter.from_path(
            obj_path,
            args.dunder
        )
        print(documenter)


def main():
    description = "Python Documentation Utility"
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('object_name', nargs="?", help='files', default="")
    parser.add_argument('-r', dest="dunder", action="store_true", default=False,
                        help="show dunder methods")
    try:
        Inspecter().run(parser.parse_args())
    except Exception as E:
        print(f"Error:\n {str(E)}")

if __name__ == "__main__":
    main()
