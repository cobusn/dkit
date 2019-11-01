***
etl
***

.. toctree::
   :maxdepth: 2

model
=====

.. automodule:: dkit.etl.model

Connection
----------

.. image:: ../images/Connection.svg
	:align: center

.. autoclass:: dkit.etl.model.Connection
   :members:

Endpoint
--------

.. image:: ../images/Endpoint.svg
	:align: center

.. autoclass:: dkit.etl.model.Endpoint
   :members:

Entity
------

.. image:: ../images/Entity.svg
	:align: center

.. autoclass:: dkit.etl.model.Entity
    :members:
  
    .. automethod:: __call__
    
Query
-----

.. image:: ../images/Query.svg
	:align: center

.. autoclass:: dkit.etl.model.Query
   :members:

Relation
--------

.. image:: ../images/Relation.svg
	:align: center

.. autoclass:: dkit.etl.model.Relation
   :members:

Transform
---------

.. image:: ../images/Transform.svg
	:align: center

.. autoclass:: dkit.etl.model.Transform
    :members:
    
    .. automethod:: __call__

ModelManager
------------

.. image:: ../images/ModelManager.svg
	:align: center

.. autoclass:: dkit.etl.model.ModelManager
   :members:
   :inherited-members:

ETLServices
-------------

.. image:: ../images/ETLServices.svg
	:align: center

.. autoclass:: dkit.etl.model.ETLServices
   :members:
   :inherited-members:

schema
======

.. automodule:: dkit.etl.schema

EntityValidator
----------------

.. image:: ../images/EntityValidator.svg
	:align: center

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

.. autoclass:: dkit.etl.source.AbstractSource

.. image:: ../images/AbstractSource.svg
	:align: center

.. autoclass:: dkit.etl.source.AbstractSource
   :members:
   :undoc-members:
   :inherited-members:


FileListingSource
-----------------

.. autoclass:: dkit.etl.source.FileListingSource

.. image:: ../images/FileListingSource.svg
	:align: center

.. autoclass:: dkit.etl.source.FileListingSource
   :members:
   :undoc-members:
   :inherited-members:

AbstractMultiReaderSource
-------------------------

.. autoclass:: dkit.etl.source.AbstractMultiReaderSource

.. image:: ../images/AbstractMultiReaderSource.svg
	:align: center

.. autoclass:: dkit.etl.source.AbstractMultiReaderSource
   :members:
   :undoc-members:
   :inherited-members:

CsvDictSource
-------------

.. autoclass:: dkit.etl.source.CsvDictSource

. image:: ../images/CsvDictSource.svg
	:align: center

. autoclass:: dkit.etl.source.CsvDictSource
   :members:
   :undoc-members:
   :inherited-members:

JsonlSource
-----------

.. image:: ../images/JsonlSource.svg
	:align: center

.. autoclass:: dkit.etl.source.JsonlSource
   :members:
   :undoc-members:
   :inherited-members:

PickleSource
------------

.. image:: ../images/PickleSource.svg
	:align: center

.. autoclass:: dkit.etl.source.PickleSource
   :members:
   :undoc-members:
   :inherited-members:

Transforms
==========

.. automodule:: dkit.etl.transform


.. autoclass:: dkit.etl.transform.FormulaTransform
   :members:
   :undoc-members:
   :inherited-members:


Extensions
==========

HDFS
----

.. automodule:: dkit.etl.extensions.ext_hdfs

HDFSReader
~~~~~~~~~~
.. autoclass:: dkit.etl.extensions.ext_hdfs.HDFSReader
   :members:
   :undoc-members:
   :inherited-members:

HDFSWriter
~~~~~~~~~~
.. autoclass:: dkit.etl.extensions.ext_hdfs.HDFSWriter
   :members:
   :undoc-members:
   :inherited-members:

PyTables
--------

PyTablesAccessor
~~~~~~~~~~~~~~~~~~
.. image:: ../images/PyTablesAccessor.svg
	:align: center

.. autoclass:: dkit.etl.extensions.ext_tables.PyTablesAccessor
   :members:
   :undoc-members:
   :inherited-members:

PyTablesModelFactory
~~~~~~~~~~~~~~~~~~~~~~
.. image:: ../images/PyTablesModelFactory.svg
	:align: center

.. autoclass:: dkit.etl.extensions.ext_tables.PyTablesModelFactory
   :members:
   :undoc-members:
   :inherited-members:

PyTablesSource
~~~~~~~~~~~~~~
.. image:: ../images/PyTablesSource.svg
	:align: center

.. autoclass:: dkit.etl.extensions.ext_tables.PyTablesSource
   :members:
   :undoc-members:
   :inherited-members:

PyTablesSink
~~~~~~~~~~~~
.. image:: ../images/PyTablesSink.svg
	:align: center

.. autoclass:: dkit.etl.extensions.ext_tables.PyTablesSink
   :members:
   :undoc-members:
   :inherited-members:

PyTablesReflector
~~~~~~~~~~~~~~~~~
.. image:: ../images/PyTablesReflector.svg
	:align: center

.. autoclass:: dkit.etl.extensions.ext_tables.PyTablesReflector
   :members:
   :undoc-members:
   :inherited-members:

PyTablesServices
~~~~~~~~~~~~~~~~
.. image:: ../images/PyTablesServices.svg
	:align: center

.. autoclass:: dkit.etl.extensions.ext_tables.PyTablesServices
   :members:
   :undoc-members:
   :inherited-members:

SqlAlchemy
----------
Abstraction of the SQLAlchemy API that provide access to any supported database. 
Refer to the SQLAlchemy documentaton for more information.

Sample usage:

.. include:: ../../examples/example_ext_sql_alchemy.py
    :literal:

Produces:

.. include:: ../../examples/example_ext_sql_alchemy.out
    :literal:


SQLAlchemyAccessor
~~~~~~~~~~~~~~~~~~
.. image:: ../images/SQLAlchemyAccessor.svg
	:align: center

.. autoclass:: dkit.etl.extensions.ext_sql_alchemy.SQLAlchemyAccessor
   :members:
   :undoc-members:
   :inherited-members:

SQLAlchemyModelFactory
~~~~~~~~~~~~~~~~~~~~~~
.. image:: ../images/SQLAlchemyModelFactory.svg
	:align: center

.. autoclass:: dkit.etl.extensions.ext_sql_alchemy.SQLAlchemyModelFactory
   :members:
   :undoc-members:
   :inherited-members:

SQLAlchemyTableSource
~~~~~~~~~~~~~~~~~~~~~
.. image:: ../images/SQLAlchemyTableSource.svg
	:align: center

.. autoclass:: dkit.etl.extensions.ext_sql_alchemy.SQLAlchemyTableSource
   :members:
   :undoc-members:
   :inherited-members:

SQLAlchemySelectSource
~~~~~~~~~~~~~~~~~~~~~~
.. image:: ../images/SQLAlchemySelectSource.svg
	:align: center

.. autoclass:: dkit.etl.extensions.ext_sql_alchemy.SQLAlchemySelectSource
   :members:
   :undoc-members:
   :inherited-members:

SQLAlchemySink
~~~~~~~~~~~~~~
.. image:: ../images/SQLAlchemySink.svg
	:align: center

.. autoclass:: dkit.etl.extensions.ext_sql_alchemy.SQLAlchemySink
   :members:
   :undoc-members:
   :inherited-members:

SQLAlchemaReflector
~~~~~~~~~~~~~~~~~~~
.. image:: ../images/SQLAlchemyReflector.svg
	:align: center

.. autoclass:: dkit.etl.extensions.ext_sql_alchemy.SQLAlchemyReflector
   :members:
   :undoc-members:
   :inherited-members:

SQLServices
~~~~~~~~~~~

.. autoclass:: dkit.etl.extensions.ext_sql_alchemy.SQLServices
   :members:
   :undoc-members:
   :inherited-members:

XML
---

XmlSource
~~~~~~~~~
.. image:: ../images/XmlSource.svg
	:align: center

.. autoclass:: dkit.etl.extensions.ext_xml.XmlSource
   :members:
   :undoc-members:
   :inherited-members:

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

multi_processing
================
Abstraction of the MultiProcessing API that assist in running transforms
in multiple processes.

Sample usage:

.. include:: ../../examples/example_etl_multiprocessing.py
    :literal:

Coordinator
-----------
.. image:: ../images/Coordinator.svg
	:align: center

.. autoclass:: dkit.etl.multi_processing.Coordinator
   :members:
   :undoc-members:
   :inherited-members:

Worker
------
.. image:: ../images/Worker.svg
	:align: center

.. autoclass:: dkit.etl.multi_processing.Worker
   :members:
   :undoc-members:
   :inherited-members:

verifier
========

ShelveVerifier
--------------

.. image:: ../images/ShelveVerifier.svg
	:align: center

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

