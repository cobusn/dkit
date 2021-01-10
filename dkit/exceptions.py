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

"""
library exceptions
"""


class DKitException(Exception):
    pass


class DKitDocumentException(DKitException):
    pass


class DKitPlotException(DKitException):
    pass


class DkitGrammarException(DKitException):
    pass


class DKitShellException(Exception):
    pass


class DKitArgumentException(Exception):
    pass


class DKitApplicationException(DKitException):
    pass


class DKitConfigException(DKitException):
    pass


class DKitDataException(DKitException):
    pass


class DKitETLException(DKitException):
    pass


class DKitParseException(DKitException):
    pass


class DKitTimeoutException(DKitException):
    pass


class DKitValidationException(DKitException):
    """
    exception related to schema validation
    """
    pass
