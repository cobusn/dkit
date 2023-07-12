# Copyright (c) 2023 Cobus Nel
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
Console app runners
"""
import os
import inspect


class DispatcherRunner:
    """
    dispatch methods based on commands specified in the args

    The class will call a 'do_xxx()' method based on the command
    specified in the parser

    e.g:

        parser = argparse.ArgumentParser(prog='ferret', description=description)
        sub_parsers = parser.add_subparsers(
            help='sub-command help',
            dest="command",
        )

        # print command
        parser_enc = sub_parsers.add_parser('print', help='print item')

    For the above, a sublcass must have a `do_print()` method

    Notes:
        - debug mode (break on error) when:
            * DK_DEBUG environmental variable specified
            * 'debug' argument specified and true
    """

    def __init__(self, args, arg='command'):
        self.args = args
        self.arg = arg

    def debug_enabled(self):
        if bool(os.environ.get("DK_DEBUG", False)):
            return True
        elif hasattr(self.args, "debug") and self.args.debug is True:
            return True
        else:
            return False

    def get_method(self, name):
        """get module name"""
        candidates = [
            i for i in
            inspect.getmembers(self, predicate=inspect.ismethod)
            if i[0].startswith(f"do_{name.strip()}")
        ]
        if len(candidates) == 1:
            return candidates[0][1]
        elif len(candidates) > 1:
            raise Exception(
                "Ambiguous options: {}".format(", ".join(candidates))
            )
        else:
            raise Exception(f"Unrecognized command: {name}")

    def run(self):
        method_name = getattr(self.args, self.arg)
        method = self.get_method(method_name)
        if self.debug_enabled():
            return method()
        else:
            try:
                return method()
            except KeyError as err:
                print(f"Invalid command {method}: {err}")
            except Exception as err:
                print(f"An exception occurred: {err.__class__.__name__} {err}")
