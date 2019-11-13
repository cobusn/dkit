*******
parsers
*******

.. toctree::
   :maxdepth: 2

HTMLTableParser
===============

.. image:: ../images/HTMLTableParser.svg
	:align: center

.. autoclass:: dkit.parsers.html_parser.HTMLTableParser
   :members:
   :undoc-members:

MatchScanner
============
Refer to `SearchScanner` for usage example.

.. image:: ../images/MatchScanner.svg
	:align: center

.. autoclass:: dkit.parsers.helpers.MatchScanner
   :members:
   :undoc-members:
   :inherited-members:

SearchScanner
=============

.. image:: ../images/SearchScanner.svg
	:align: center

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

.. image:: ../images/InfixParser.svg
	:align: center

.. autoclass:: dkit.parsers.infix_parser.InfixParser
   :members:
   :undoc-members:
   :inherited-members:

ExpressionParser
================

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
.. image::  ../images/URIStruct.svg
    :align: center

.. autoclass:: dkit.parsers.uri_parser.URIStruct
    :members:
    :undoc-members:

parse
-----
.. autofunction:: dkit.parsers.uri_parser.parse

type_parser
===========

.. image:: ../images/TypeParser.svg
    :align: center

.. autoclass:: dkit.parsers.type_parser.TypeParser


* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
