****************
multi_processing
****************

.. toctree::
   :maxdepth: 2

.. automodule:: dkit.multi_processing

The module consist of the following components:

    .. image:: ../images/classes_multiprocessing.svg
        :align: center

Pipelines
=========

ListPipeline
------------

.. autoclass:: dkit.multi_processing.ListPipeline
   :members:
   :undoc-members:
   :inherited-members:

.. image:: ../images/ListPipeline.svg
	:align: center

This example illustrate use of the list oriented pipeline:

    .. include:: ../../examples/example_list_pipeline.py
        :literal:

This example will generate the following output:

    .. include:: ../../examples/example_list_pipeline.out
        :literal:

TaskPipeline
------------
.. autoclass:: dkit.multi_processing.TaskPipeline
   :members:
   :undoc-members:
   :inherited-members:

.. image:: ../images/TaskPipeline.svg
	:align: center

This example illustrate use of the task oriented pipeline:

    .. include:: ../../examples/example_task_pipeline.py
        :literal:

This example will generate the following output:

    .. include:: ../../examples/example_task_pipeline.out
        :literal:

Message types
=============

ListMessage
-----------
`ListMessage` objects are used with the ListPipeline and contain
a list of objects.

.. image:: ../images/ListMessage.svg
	:align: center

.. autoclass:: dkit.multi_processing.ListMessage
   :members:
   :undoc-members:
   :inherited-members:
   :special-members: __iter__

UIDTaskMessage
--------------
Used with `TaskPipeline` instances.  The message id will
be a randomly generated identifier.

.. image:: ../images/UIDTaskMessage.svg
	:align: center

.. autoclass:: dkit.multi_processing.UIDTaskMessage
   :members:
   :undoc-members:
   :inherited-members:

MD5TaskMessage
--------------
Used with `TaskPipeline` instances.  The message id will
be an md5 hash of its arguments. 

.. warning::
   messages with identical arguments will be discarded.
   if this is not the intented behaviour, use the 
   UIDTaskMessage instead.

.. image:: ../images/MD5TaskMessage.svg
	:align: center

.. autoclass:: dkit.multi_processing.MD5TaskMessage
   :members:
   :undoc-members:
   :inherited-members:

Workers
=======
Worker classes need to be defined by the user and must inherit from `Worker`
(refer to examples above).

.. image:: ../images/Worker.svg
	:align: center

.. autoclass:: dkit.multi_processing.Worker
   :members:
   :undoc-members:
   :inherited-members:

Journal
=======
The Journal class is used internally by the library although it can be 
instantiated from a Shelve file

.. image:: ../images/Journal.svg
	:align: center

.. autoclass:: dkit.multi_processing.Journal
   :members:
   :undoc-members:
   :inherited-members:

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
