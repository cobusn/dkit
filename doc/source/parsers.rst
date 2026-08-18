*******
parsers
*******

.. toctree::
   :maxdepth: 2

HTMLTableParser
===============

.. inheritance-diagram:: dkit.parsers.html_parser.HTMLTableParser

.. autoclass:: dkit.parsers.html_parser.HTMLTableParser
   :members:
   :undoc-members:

MatchScanner
============
Refer to `SearchScanner` for usage example.

.. inheritance-diagram:: dkit.parsers.helpers.MatchScanner

.. autoclass:: dkit.parsers.helpers.MatchScanner
   :members:
   :undoc-members:
   :inherited-members:

SearchScanner
=============

.. inheritance-diagram:: dkit.parsers.helpers.SearchScanner

.. autoclass:: dkit.parsers.helpers.SearchScanner
   :members:
   :undoc-members:
   :inherited-members:

Example Usage
-------------
The example below illustrate a primitive parser. Note the limitation
of this particular parser is that each item is scanned only once:

    .. include:: ../../examples/example_string_scanner.py
        :literal:

This example will generate the following output:

    .. include:: ../../examples/example_string_scanner.out
        :literal:
 
InfixParser
===========

.. inheritance-diagram:: dkit.parsers.infix_parser.InfixParser

.. autoclass:: dkit.parsers.infix_parser.InfixParser
   :members:
   :undoc-members:
   :inherited-members:

ExpressionParser
================

.. inheritance-diagram:: dkit.parsers.infix_parser.ExpressionParser

.. autoclass:: dkit.parsers.infix_parser.ExpressionParser
   :members:
   :undoc-members:
   :inherited-members:

Example usage:

.. include:: ../../examples/example_expression_parser.py
    :literal:

The above will produce the following output:

.. include:: ../../examples/example_expression_parser.out
    :literal:

uri_parser
==========

URIStruct
---------
.. inheritance-diagram:: dkit.parsers.uri_parser.URIStruct

.. autoclass:: dkit.parsers.uri_parser.URIStruct
    :members:
    :undoc-members:

parse
-----
.. autofunction:: dkit.parsers.uri_parser.parse

type_parser
===========

.. inheritance-diagram:: dkit.parsers.type_parser.TypeParser

.. autoclass:: dkit.parsers.type_parser.TypeParser


* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
