
data
****

.. toctree::
   :maxdepth: 2

aggregation
===========

.. automodule:: dkit.data.aggregation

.. image:: ../images/classes_aggregation.svg
	:align: center

Example
-------
The following example illustrate it use:

.. literalinclude:: ../../examples/example_aggregate.py
   :language: python

And produce this output:

.. include:: ../../examples/example_aggregate.out
   :literal:


Aggregate
---------

.. image:: ../images/Aggregate.svg
	:align: center

.. autoclass:: dkit.data.aggregation.Aggregate
   :members:
   :undoc-members:

GroupBy
-------

.. image:: ../images/GroupBy.svg
	:align: center

.. autoclass:: dkit.data.aggregation.GroupBy
   :members:
   :undoc-members:

Count
-----

.. image:: ../images/Count.svg
	:align: center

.. autoclass:: dkit.data.aggregation.Count
   :members:
   :undoc-members:

Std
---

.. autoclass:: dkit.data.aggregation.Std
   :members:
   :undoc-members:

Sum
---

.. autoclass:: dkit.data.aggregation.Sum
   :members:
   :undoc-members:

IQR
---

.. autoclass:: dkit.data.aggregation.IQR
   :members:
   :undoc-members:

Mean
----

.. autoclass:: dkit.data.aggregation.Mean
   :members:
   :undoc-members:

Min
---

.. autoclass:: dkit.data.aggregation.Min
   :members:
   :undoc-members:

Max
---

.. autoclass:: dkit.data.aggregation.Max
   :members:
   :undoc-members:

Var
---

.. autoclass:: dkit.data.aggregation.Var
   :members:
   :undoc-members:

bxr
===
.. automodule:: dkit.data.bxr
    :members:
    :undoc-members:

containers
==========

AttrDict
--------

.. image:: ../images/AttrDict.svg
	:align: center

.. autoclass:: dkit.data.containers.AttrDict
   :members:
   :undoc-members:

DictonaryEmulator
-----------------

.. image:: ../images/DictionaryEmulator.svg
	:align: center

.. autoclass:: dkit.data.containers.DictionaryEmulator
   :members:
   :undoc-members:
   :inherited-members:

SortedCollection
----------------

.. image:: ../images/SortedCollection.svg
   :align: center

.. autoclass:: dkit.data.containers.SortedCollection
   :members:
   :undoc-members:
   :inherited-members:
   
_Shelve
-------

.. image:: ../images/_Shelve.svg
   :align: center

.. autoclass:: dkit.data.containers._Shelve
   :members:
   :undoc-members:
   :inherited-members:

FlexShelve
----------

.. image:: ../images/FlexShelve.svg
   :align: center

.. autoclass:: dkit.data.containers.FlexShelve
   :members:
   :undoc-members:
   :inherited-members:

FastFlexShelve
--------------

.. image:: ../images/FlexShelve.svg
   :align: center

.. autoclass:: dkit.data.containers.FlexShelve
   :members:
   :undoc-members:
   :inherited-members:



FlexBSDDBShelve
---------------

.. image:: ../images/FlexBSDDBShelve.svg
   :align: center

.. autoclass:: dkit.data.containers.FlexBSDDBShelve
   :members:
   :undoc-members:
   :inherited-members:


FastFlexShelve
--------------

.. image:: ../images/FastFlexShelve.svg
   :align: center

.. autoclass:: dkit.data.containers.FastFlexShelve
   :members:
   :undoc-members:
   :inherited-members:



fake
====

.. automodule:: dkit.data.fake_helper
   :members:
   :undoc-members:

filters
=======

search_filter
-------------

.. autofunction:: dkit.data.filters.search_filter

match_filter
------------

.. autofunction:: dkit.data.filters.match_filter

ExpressionFilter
----------------

.. image:: ../images/ExpressionFilter.svg
    :align: center

.. autoclass:: dkit.data.filters.ExpressionFilter

Example usage:

.. include:: ../../examples/example_expression_filter.py
    :literal:


Proxy
-----

.. autoclass:: dkit.data.filters.Proxy

.. image:: ../images/Proxy.svg
	:align: center

histogram
=========
.. automodule:: dkit.data.histogram

Example usage:

.. literalinclude:: ../../examples/example_histogram.py
   :language: python

Will produce the following:

.. literalinclude:: ../../examples/example_histogram.out

Bin
---
.. autoclass:: dkit.data.histogram.Bin

.. image:: ../images/Bin.svg
	:align: center

Histogram
---------
.. autoclass:: dkit.data.histogram.Histogram

.. image:: ../images/Histogram.svg
	:align: center

map_db
======

.. automodule:: dkit.data.map_db

Object
------

.. image:: ../images/Object.svg
	:align: center

.. autoclass:: dkit.data.map_db.Object
   :members:
   :undoc-members:
   :inherited-members:

Example
-------
The following example illustrate it use:

.. literalinclude:: ../../examples/example_object_map.py
   :language: python

And produce this output:

.. include:: ../../examples/example_object_map.out
   :literal:

ObjectMap
---------

.. image:: ../images/ObjectMap.svg
	:align: center

.. autoclass:: dkit.data.map_db.ObjectMap
   :members:
   :undoc-members:
   :inherited-members:

ObjectMapDB
-----------

.. image:: ../images/ObjectMapDB.svg
	:align: center

.. autoclass:: dkit.data.map_db.ObjectMapDB
   :members:
   :undoc-members:
   :inherited-members:

FileLoaderMixin
---------------

.. image:: ../images/FileLoaderMixin.svg
	:align: center

.. autoclass:: dkit.data.map_db.FileLoaderMixin
   :members:
   :undoc-members:
   :inherited-members:
 
FileObjectMapDB
---------------

.. image:: ../images/FileObjectMapDB.svg
	:align: center

.. autoclass:: dkit.data.map_db.FileObjectMapDB
   :members:
   :undoc-members:
   :inherited-members:


manipulate
==========   

.. automodule:: dkit.data.manipulate

aggregate
---------
.. autofunction:: dkit.data.manipulate.aggregate

aggregates
----------
.. autofunction:: dkit.data.manipulate.aggregates

ReducePivot
-----------

.. image:: ../images/ReducePivot.svg
   :align: center

.. autoclass:: dkit.data.manipulate.ReducePivot
   :members:
   :undoc-members:

merge
-----
.. autofunction:: dkit.data.manipulate.merge

iter_sample
-----------
.. autofunction:: dkit.data.manipulate.iter_sample

Pivot
-----
.. image:: ../images/Pivot.svg
   :align: center

.. autoclass:: dkit.data.manipulate.Pivot
   :members:
   :undoc-members:

Substitute
----------

.. image:: ../images/Substitute.svg
   :align: center

.. autoclass:: dkit.data.manipulate.Substitute
   :members:
   :undoc-members:
   :special-members:
   :exclude-members: __dict__,__weakref__,__module__


matching
========

.. automodule:: dkit.data.matching

DictMatcher
-----------

.. image:: ../images/DictMatcher.svg
    :align: center

.. autoclass:: dkit.data.matching.DictMatcher
   :members:
   :undoc-members:
   :inherited-members:

FieldSpec
---------

.. image:: ../images/FieldSpec.svg
    :align: center

.. autoclass:: dkit.data.matching.FieldSpec
   :members:
   :undoc-members:
   :inherited-members:

inner_join
----------

.. autofunction:: dkit.data.matching.inner_join

unmatched_left
--------------

.. autofunction:: dkit.data.matching.unmatched_left

unmatched_right
---------------

.. autofunction:: dkit.data.matching.unmatched_right

stats
=====

.. automodule:: dkit.data.stats

Accumulator
-----------

.. image:: ../images/Accumulator.svg
    :align: center

.. autoclass:: dkit.data.stats.Accumulator
   :members:
   :undoc-members:
   :inherited-members:

xml
===

XmlTransformer
--------------

.. autoclass:: dkit.data.xml_helper.XmlTransformer

Class Diagram
~~~~~~~~~~~~~
.. image:: ../images/XmlTransformer.svg
	:align: center

Members
~~~~~~~
.. autoclass:: dkit.data.xml_helper.XmlTransformer
   :members:
   :undoc-members:
   :inherited-members:

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
