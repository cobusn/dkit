# Copyright (c) 2017 Cobus Nel
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
XML utilities
"""

from ..parsers.infix_parser import AbstractFieldParser
import io
import importlib
import collections
from xml.sax.handler import ContentHandler
from xml import sax
from ..utilities.instrumentation import CounterLogger
from ..exceptions import CkitParseException
import typing
from .. import NA_VALUE
#
# NOTE
#
# lxml.etree is lazily imported below
#


def striptag(tag):
    """
    strip namespace from tag
    """
    if "}" in tag:
        return tag.split("}")[1]
    else:
        return tag


def etree_to_dict(tree, ns=False, transform_attributes=True):
    """
    transform element tree to dictionary

    :param transform_attributes: Aadd "@" in front of attribute name
    """
    tag = striptag(tree.tag)
    d = {tag: {} if tree.attrib else None}

    # children
    children = list(tree)
    if children:
        dd = collections.defaultdict(list)
        for dc in map(etree_to_dict, children):
            for k, v in dc.items():
                dd[k].append(v)
        d = {tag: {k: v[0] if len(v) == 1 else v for k, v in dd.items()}}

    # attribs
    if tree.attrib:
        if transform_attributes:
            d[tag].update(('@' + striptag(k), v) for k, v in tree.attrib.items())
        else:
            d[tag].update((striptag(k), v) for k, v in tree.attrib.items())

    # tet
    if tree.text:
        text = tree.text.strip()
        if children or tree.attrib:
            if text:
                d[tag]['#text'] = text
        else:
            d[tag] = text

    return d


class XmlFieldParser(AbstractFieldParser):
    """
    extend AbstractFieldParser to provide an
    **xpath** function.

    Used interanally by XmlTransformer and not part
    of published API
    """
    def __init__(self, template, all_fields, none_value=NA_VALUE):
        super().__init__(none_value=none_value)
        self._functions["xpath"] = self.__xpath_closure_fn
        self.all_fields = all_fields
        self.element = None
        self.parse(template)

    def _get_variable(self, variable_name):
        return self.all_fields[variable_name].evaluates(self.element)

    def __xpath_closure_fn(self, parser, strg, tokens):
        from lxml.etree import XPath
        fname = tokens[0]
        if len(tokens) != 2:
            raise CkitParseException(f"function {fname} require 1 parameter")
        xpath_query = tokens[1]
        compiled = XPath(xpath_query)

        # The xpath will be the last item on the parse stack. Since
        # this is in the closure we need to remove it from the parse
        # stack..
        parser._parse_stack.pop()

        def do_query(parser):
            rv = compiled(parser.element)
            if rv:
                return str(rv[0])
            else:
                return self.none_value

        return do_query

    def _deprecated_xpath_query(self, path):
        """
        return value from xpath query
        """
        try:
            retval = self.element.xpath(path)[0]
        except IndexError:
            retval = self.none_value
        return retval

    def evaluates(self, element):
        self.element = element
        return super().eval()


class XmlTransformer(object):
    """
    Transform XML to json given a transform specification.

    Two paramers are provided, the name of the boundary
    tag and a dictionary with field specfications. The following
    example provide an example of using the transformer:

    Sample usage:

     .. include:: ../../examples/example_xml_transformer.py
        :literal:

    Produces:

      .. include:: ../../examples/example_xml_transformer.out
        :literal:

    :param boundary: "path for xml boundary tag"
    :param fields_dict: dictionary with field specification

    """
    def __init__(self, boundary, fields_dict=None):
        self.boundary = boundary
        if fields_dict is None:
            self.fields_dict = {}
        else:
            self.fields_dict = fields_dict
        self.recipe = {}
        self._build_recipe(self.fields_dict)
        self.et = importlib.import_module("lxml.etree")

    def _build_recipe(self, template):
        """
        Build the process recipe
        """
        for key, script in self.fields_dict.items():
            self.recipe[key] = XmlFieldParser(script, self.recipe)

    def _iter_document(self, doc):
        """
        parse xml elements and yield
        parsed rows.
        """
        # note on optimisation.
        # this function include logic described in listing:
        # https://www.ibm.com/developerworks/xml/library/x-hiperfparse/
        # the logic free up memory from completed nodes
        recipe = self.recipe
        for (event, node) in self.et.iterparse(doc, tag=self.boundary):
            yield {key: parser.evaluates(node) for key, parser in recipe.items()}
            node.clear()
            while node.getprevious() is not None:
                del node.getparent()[0]

    def iter_string(self, the_string):
        """
        iterate through an provided xml string.

        :param the_string: string containing xml
        """
        doc = io.BytesIO(the_string.encode("utf-8"))
        yield from self._iter_document(doc)

    def iter_file(self, file_obj):
        """
        iterate though elements in an
        XML file object.

        :param file_obj: file object for xml data
        """
        yield from self._iter_document(file_obj)


class XMLStat(ContentHandler):
    """
    returns a Counter object that count instances
    of each path in an XML file
    """
    class __XMLStatHandler(ContentHandler):

        def __init__(self, stats_instance):
            self.stack = []
            self.stats = stats_instance
            self.counter = collections.Counter()

        def startElement(self, name, attributes):
            self.stack.append(name)
            key = "/".join(self.stack)
            self.counter[key] += 1
            for k in attributes.keys():
                attr_key = f"{key}/@{k}"
                self.counter[attr_key] += 1

        def endElement(self, name):
            self.stats.increment()
            self.stack.pop()

    def __init__(self, log_trigger=10000):
        self.stats = CounterLogger(logger=__name__, trigger=log_trigger)

    def process_stream(self, stream: typing.TextIO) -> collections.Counter:
        """
        process a file like object

        args:
            stream: python file like object

        returns:
            collections.Counter
        """
        self.stats.start()
        handler = self.__XMLStatHandler(self.stats)
        sax.parse(stream, handler)
        self.stats.stop()
        return handler.counter

    def process_file(self, file_name: str) -> collections.Counter:
        """
        open and process a filename
        """
        with open(file_name) as in_stream:
            return self.process_stream(in_stream)

    def __call__(self, stream: typing.TextIO) -> collections.Counter:
        return self.process_stream(stream)
