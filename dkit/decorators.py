# Copyright (c) 2019 Cobus Nel
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
Decorators

=========== =============== =================================================
Aug 2019    Cobus Nel       Initial version
=========== =============== =================================================
"""
import functools
import sys
import time
import warnings


__all__ = ["deprecated", "timeit"]


def deprecated(message=None):
    """
    Marks deprecated functions
    """
    def outer_fn(the_function):
        msg = f"{the_function.__name__} is deprecated"
        if message:
            msg = f"msg: {message}."
        if the_function.__doc__ is None:
            the_function.__doc__ = msg

        @functools.wraps(the_function)
        def inner(*args, **kwargs):
            warnings.warn(msg, category=DeprecationWarning, stacklevel=2)
            return the_function(*args, **kwargs)

        return inner
    return outer_fn


def timeit(f):
    """
    time duration of function all every time it is called
    """
    @functools.wraps(f)
    def wrap(*args, **kw):
        ts = time.perf_counter()
        result = f(*args, **kw)
        te = time.perf_counter()
        sys.stderr.write(f"function: '{f.__name__}' took: {te-ts:.2f} sec to complete.\n")
        return result
    return wrap
