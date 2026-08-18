
data
****

.. toctree::
   :maxdepth: 2

aggregation
===========

.. automodule:: dkit.data.aggregation

.. inheritance-diagram:: dkit.data.aggregation

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

.. inheritance-diagram:: dkit.data.aggregation.Aggregate

.. autoclass:: dkit.data.aggregation.Aggregate
   :members:
   :undoc-members:

GroupBy
-------

.. inheritance-diagram:: dkit.data.aggregation.GroupBy

.. autoclass:: dkit.data.aggregation.GroupBy
   :members:
   :undoc-members:

Count
-----

.. inheritance-diagram:: dkit.data.aggregation.Count

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

.. inheritance-diagram:: dkit.data.containers.AttrDict

.. autoclass:: dkit.data.containers.AttrDict
   :members:
   :undoc-members:

DictonaryEmulator
-----------------

.. inheritance-diagram:: dkit.data.containers.DictionaryEmulator

.. autoclass:: dkit.data.containers.DictionaryEmulator
   :members:
   :undoc-members:
   :inherited-members:

SortedCollection
----------------

.. inheritance-diagram:: dkit.data.containers.SortedCollection

.. autoclass:: dkit.data.containers.SortedCollection
   :members:
   :undoc-members:
   :inherited-members:
   
_Shelve
-------

.. inheritance-diagram:: dkit.data.containers._Shelve
   :private-bases:

.. autoclass:: dkit.data.containers._Shelve
   :members:
   :undoc-members:
   :inherited-members:

FlexShelve
----------

.. inheritance-diagram:: dkit.data.containers.FlexShelve

.. autoclass:: dkit.data.containers.FlexShelve
   :members:
   :undoc-members:
   :inherited-members:

FastFlexShelve
--------------

.. inheritance-diagram:: dkit.data.containers.FastFlexShelve

.. autoclass:: dkit.data.containers.FastFlexShelve
   :members:
   :undoc-members:
   :inherited-members:

FlexBSDDBShelve
---------------

.. inheritance-diagram:: dkit.data.containers.FlexBSDDBShelve

.. autoclass:: dkit.data.containers.FlexBSDDBShelve
   :members:
   :undoc-members:
   :inherited-members:


OrderedSet
----------

.. inheritance-diagram:: dkit.data.containers.OrderedSet

.. autoclass:: dkit.data.containers.OrderedSet
   :members:
   :undoc-members:
   :inherited-members:

diff
====

.. automodule:: dkit.data.diff
   :members:
   :undoc-members:

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

.. inheritance-diagram:: dkit.data.filters.ExpressionFilter

.. autoclass:: dkit.data.filters.ExpressionFilter

Example usage:

.. include:: ../../examples/example_expression_filter.py
    :literal:


Proxy
-----

.. autoclass:: dkit.data.filters.Proxy

.. inheritance-diagram:: dkit.data.filters.Proxy

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

.. inheritance-diagram:: dkit.data.histogram.Bin

Histogram
---------
.. autoclass:: dkit.data.histogram.Histogram

.. inheritance-diagram:: dkit.data.histogram.Histogram

map_db
======

.. automodule:: dkit.data.map_db

Object
------

.. inheritance-diagram:: dkit.data.map_db.Object

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

.. inheritance-diagram:: dkit.data.map_db.ObjectMap

.. autoclass:: dkit.data.map_db.ObjectMap
   :members:
   :undoc-members:
   :inherited-members:

ObjectMapDB
-----------

.. inheritance-diagram:: dkit.data.map_db.ObjectMapDB

.. autoclass:: dkit.data.map_db.ObjectMapDB
   :members:
   :undoc-members:
   :inherited-members:

FileLoaderMixin
---------------

.. inheritance-diagram:: dkit.data.map_db.FileLoaderMixin

.. autoclass:: dkit.data.map_db.FileLoaderMixin
   :members:
   :undoc-members:
   :inherited-members:
 
FileObjectMapDB
---------------

.. inheritance-diagram:: dkit.data.map_db.FileObjectMapDB

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

.. inheritance-diagram:: dkit.data.manipulate.ReducePivot

.. autoclass:: dkit.data.manipulate.ReducePivot
   :members:
   :undoc-members:

merge
-----
.. autofunction:: dkit.data.manipulate.merge

Pivot
-----
.. inheritance-diagram:: dkit.data.manipulate.Pivot

.. autoclass:: dkit.data.manipulate.Pivot
   :members:
   :undoc-members:

Substitute
----------

.. inheritance-diagram:: dkit.data.manipulate.Substitute

.. autoclass:: dkit.data.manipulate.Substitute
   :members:
   :undoc-members:
   :special-members:
   :exclude-members: __dict__,__weakref__,__module__


iteration
=========
.. automodule:: dkit.data.iteration
    :members:
    :undoc-members:


infer
=====
.. automodule:: dkit.data.infer

Supporting Classes
------------------

.. autoclass:: dkit.data.infer.Field
    :members:
    :undoc-members:

.. autoclass:: dkit.data.infer.TypeStats
    :members:
    :undoc-members:

InferSchema
-----------

.. inheritance-diagram:: dkit.data.infer.InferSchema

.. autoclass:: dkit.data.infer.InferSchema
   :members:
   :undoc-members:
   :special-members: __call__, __len__

infer_type
----------
.. autofunction:: infer_type

matching
========

.. automodule:: dkit.data.matching

DictMatcher
-----------

.. inheritance-diagram:: dkit.data.matching.DictMatcher

.. autoclass:: dkit.data.matching.DictMatcher
   :members:
   :undoc-members:
   :inherited-members:

FieldSpec
---------

.. inheritance-diagram:: dkit.data.matching.FieldSpec

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

.. inheritance-diagram:: dkit.data.stats.Accumulator

.. autoclass:: dkit.data.stats.Accumulator
   :members:
   :undoc-members:
   :inherited-members:

xml
===

XmlTransformer
--------------

Class Diagram
~~~~~~~~~~~~~
.. inheritance-diagram:: dkit.data.xml_helper.XmlTransformer

Members
~~~~~~~
.. autoclass:: dkit.data.xml_helper.XmlTransformer
   :members:
   :undoc-members:
   :inherited-members:

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
