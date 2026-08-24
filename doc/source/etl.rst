***
etl
***

.. toctree::
   :maxdepth: 2

   etl_extensions.rst

model
=====

.. automodule:: dkit.etl.model

Connection
----------

.. inheritance-diagram:: dkit.etl.model.Connection

.. autoclass:: dkit.etl.model.Connection
   :members:

Endpoint
--------

.. inheritance-diagram:: dkit.etl.model.Endpoint

.. autoclass:: dkit.etl.model.Endpoint
   :members:

Entity
------

.. inheritance-diagram:: dkit.etl.model.Entity

.. autoclass:: dkit.etl.model.Entity
    :members:
  
    .. automethod:: __call__
    
Query
-----

.. inheritance-diagram:: dkit.etl.model.Query

.. autoclass:: dkit.etl.model.Query
   :members:

Relation
--------

.. inheritance-diagram:: dkit.etl.model.Relation

.. autoclass:: dkit.etl.model.Relation
   :members:

Transform
---------

.. inheritance-diagram:: dkit.etl.model.Transform

.. autoclass:: dkit.etl.model.Transform
    :members:
    
    .. automethod:: __call__

ModelManager
------------

.. inheritance-diagram:: dkit.etl.model.ModelManager

.. autoclass:: dkit.etl.model.ModelManager
   :members:
   :inherited-members:

ETLServices
-------------

.. inheritance-diagram:: dkit.etl.model.ETLServices

.. autoclass:: dkit.etl.model.ETLServices
   :members:
   :inherited-members:

schema
======

.. automodule:: dkit.etl.schema

EntityValidator
----------------

.. inheritance-diagram:: dkit.etl.schema.EntityValidator

.. autoclass:: dkit.etl.schema.EntityValidator
   :members:
   :undoc-members:

sink
====

functions
---------

.. autofunction:: dkit.etl.sink.load

source
======

.. automodule:: dkit.etl.source

functions
---------

.. autofunction:: dkit.etl.source.load

AbstractSource
--------------

.. inheritance-diagram:: dkit.etl.source.AbstractSource

.. autoclass:: dkit.etl.source.AbstractSource
   :members:
   :undoc-members:
   :inherited-members:


FileListingSource
-----------------

.. inheritance-diagram:: dkit.etl.source.FileListingSource

.. autoclass:: dkit.etl.source.FileListingSource
   :members:
   :undoc-members:
   :inherited-members:

AbstractMultiReaderSource
-------------------------

.. inheritance-diagram:: dkit.etl.source.AbstractMultiReaderSource

.. autoclass:: dkit.etl.source.AbstractMultiReaderSource
   :members:
   :undoc-members:
   :inherited-members:

CsvDictSource
-------------

.. inheritance-diagram:: dkit.etl.source.CsvDictSource

.. autoclass:: dkit.etl.source.CsvDictSource
   :members:
   :undoc-members:
   :inherited-members:

JsonlSource
-----------

.. inheritance-diagram:: dkit.etl.source.JsonlSource

.. autoclass:: dkit.etl.source.JsonlSource
   :members:
   :undoc-members:
   :inherited-members:

PickleSource
------------

.. inheritance-diagram:: dkit.etl.source.PickleSource

.. autoclass:: dkit.etl.source.PickleSource
   :members:
   :undoc-members:
   :inherited-members:

Transforms
==========

.. automodule:: dkit.etl.transform


.. inheritance-diagram:: dkit.etl.transform.FormulaTransform

.. autoclass:: dkit.etl.transform.FormulaTransform
   :members:
   :undoc-members:
   :inherited-members:



pyarrow extension
=================

.. automodule:: dkit.etl.extensions.ext_arrow

.. autoclass:: dkit.etl.extensions.ext_arrow.ArrowSchemaGenerator
   :members:
   :undoc-members:
   :inherited-members:

.. autofunction:: dkit.etl.extensions.ext_arrow.clear_partition_data
.. autofunction:: dkit.etl.extensions.ext_arrow.write_parquet_file

Extensions
==========

See :doc:`etl_extensions` for HDFS, PyTables, SQLAlchemy, and XML.

Verifier
========

ShelveVerifier
--------------

.. inheritance-diagram:: dkit.etl.verifier.ShelveVerifier

.. autoclass:: dkit.etl.verifier.ShelveVerifier
   :members:
   :undoc-members:
   :inherited-members:

utilities
=========

Dumper
------

.. autoclass:: dkit.etl.utilities.Dumper
   :members:
   :undoc-members:
   :inherited-members:

source_factory
--------------
.. autofunction:: dkit.etl.utilities.source_factory
  
sink_factory
------------
.. autofunction:: dkit.etl.utilities.sink_factory

open_source
-----------
.. autofunction:: dkit.etl.utilities.open_source

open_sink
---------
.. autofunction:: dkit.etl.utilities.open_sink

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

