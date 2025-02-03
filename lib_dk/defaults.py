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

from dkit.etl import model
DEFAULT_N = 0
DEFAULT_PROBABILITY = 1
DEFAULT_SAMPLE_SIZE = 1000
DEFAULT_LOG_TRIGGER = 100000
DEFAULT_MODEL_FILE = model.DEFAULT_MODEL_FILE
GLOBAL_CONFIG_FILE = model.GLOBAL_CONFIG_FILE
LOCAL_CONFIG_FILE = model.LOCAL_CONFIG_FILE
CASE_TRANSFORMS = ["upper", "lower", "camel", "same"]
TABLE_STYLE = "simple"
MSG_ERR_FILE_EXIST = "file '{}' exists already"
MSK_ERR_KEYERROR = "key {} is not valid"
MSG_IF_REMOVE_ENTITY = "remove entity: [{}]"
