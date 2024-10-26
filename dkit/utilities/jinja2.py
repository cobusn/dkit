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
from jinja2 import meta
from typing import List
import re


def is_template(string: str) -> bool:
    """return true if input string contain standard jinja2 elements"""
    jinja2_patterns = [
        r'\{\{.*?\}\}',  # Matches {{ ... }}
        r'\{\%.*?\%\}',  # Matches {% ... %}
        r'\{#.*?#\}',    # Matches {# ... #} (comments)
    ]
    for pattern in jinja2_patterns:
        if re.search(pattern, string):
            return True
    return False


def find_variables(template: str) -> List[str]:
    """
    find undeclared variables in template

    return:
        - list of variables
    """
    env = jinja2.Environment()
    ast = env.parse(template)
    return list(meta.find_undeclared_variables(ast))


def render(template: str, **variables):
    """convenience function to render a template

    instantiate and render a template, nothing else
    """
    tpl = jinja2.Template(template)
    return tpl.render(variables)


def render_strict(template: str, **variables):
    """render a template but ensure all variables are defined

    raises: jinja2.exceptions.UndefinedError
    """
    tpl = jinja2.Template(
        template,
        undefined=jinja2.StrictUndefined
    )
    return tpl.render(variables)
