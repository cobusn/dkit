#
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
#
import argparse
import importlib


class LazyLoad(object):
    """
    lazy import of modules

    args:
        * name: name of module
    """
    def __init__(self, library):
        self._library = library
        self._mod = None

    def __getattrib__(self, name):
        if not self._mod:
            self._mod = importlib.import_module(self._library)
        return getattr(self._mod, name)


def confirm(prompt=None, default=False):
    """prompts for yes or no response from the user. Returns True for yes and
    False for no.

    'default' should be set to the default value assumed by the caller when
    user simply types ENTER.

    """
    if prompt is None:
        prompt = 'Confirm'

    if default:
        prompt = '%s [%s]|%s: ' % (prompt, 'y', 'n')
    else:
        prompt = '%s [%s]|%s: ' % (prompt, 'n', 'y')

    while True:
        ans = input(prompt)
        if not ans:
            return default
        if ans not in ['y', 'Y', 'n', 'N']:
            print('please enter y or n.')
            continue
        if ans == 'y' or ans == 'Y':
            return True
        if ans == 'n' or ans == 'N':
            return False


class StoreList(argparse.Action):
    """
    helper to parse comma separated values to lists
    """
    def __call__(self, parser, namespace, values, option_string=None):
        my_list = [i.strip() for i in values.split(",")]
        setattr(namespace, self.dest, my_list)


class StoreDict(argparse.Action):
    """
    helper to parse key value pairs
    """
    def __call__(self, parser, namespace, values, option_string=None):
        my_dict = {}
        for kv in values.split(","):
            k, v = kv.split("=")
            my_dict[k] = v
        setattr(namespace, self.dest, my_dict)
