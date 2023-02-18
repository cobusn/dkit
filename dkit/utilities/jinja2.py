# Copyright (c) 2022 Cobus Nel
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
Utilities for using jinja2
"""

# from ..utilities.cmd_helper import LazyLoad
# jinja2 = LazyLoad("jinja2")
import jinja2
from typing import List


def find_variables(template: str) -> List[str]:
    """
    find undeclared variables in template

    return:
        - list of variables
    """
    env = jinja2.Environment()
    ast = env.parse(template)
    return list(jinja2.meta.find_undeclared_variables(ast))


def render_strict(template: str, **variables):
    """
    render a template but ensure all variables are
    defined
    """
    tpl = jinja2.Template(
        template,
        undefined=jinja2.StrictUndefined
    )
    return tpl.render(variables)
