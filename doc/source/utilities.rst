*********
utilities
*********

.. toctree::
   :maxdepth: 2

concurrency
===========
.. automodule:: dkit.utilities.concurrency
    :members:

file_helper
===========
.. automodule:: dkit.utilities.file_helper
    :members:

functions
---------
.. autofunction:: dkit.utilities.file_helper.temp_filename

instrumentation
===============

Counter
-------

Class Diagram
~~~~~~~~~~~~~
.. image:: ../images/Counter.svg
	:align: center

Members
~~~~~~~
.. autoclass:: dkit.utilities.instrumentation.Counter
   :members:
   :undoc-members:

CounterLogger
-------------

Class Diagram
~~~~~~~~~~~~~
.. image:: ../images/CounterLogger.svg
	:align: center

Members
~~~~~~~
.. autoclass:: dkit.utilities.instrumentation.CounterLogger
   :members:
   :undoc-members:

Exceptions
~~~~~~~~~~
.. autoclass:: dkit.utilities.instrumentation.TimerException

Timer
-----

.. image:: ../images/Timer.svg
	:align: center

.. autoclass:: dkit.utilities.instrumentation.Timer
   :members:
   :undoc-members:

.. autoclass:: dkit.utilities.instrumentation.TimerException

security
========
.. automodule:: dkit.utilities.security

Fernet
-------

.. image:: ../images/Fernet.svg
	:align: center

.. autoclass:: dkit.utilities.security.Fernet
   :members:
   :undoc-members:
   :show-inheritance:

Vigenere
--------

.. image:: ../images/Vigenere.svg
	:align: center

.. autoclass:: dkit.utilities.security.Vigenere
   :members:
   :undoc-members:
   :show-inheritance:

Pie
---

.. image:: ../images/Pie.svg
	:align: center

.. autoclass:: dkit.utilities.security.Pie
   :members:
   :undoc-members:
   :show-inheritance:

iter_helper
===========
.. automodule:: dkit.utilities.iter_helper

.. autofunction:: dkit.utilities.iter_helper.add_key
.. autofunction:: dkit.utilities.iter_helper.add_uuid_key
.. autofunction:: dkit.utilities.iter_helper.chunker
.. autofunction:: dkit.utilities.iter_helper.glob_list

log_helper
==========
.. automodule:: dkit.utilities.log_helper

functions
---------
.. autofunction:: dkit.utilities.log_helper.file_logger
.. autofunction:: dkit.utilities.log_helper.null_logger
.. autofunction:: dkit.utilities.log_helper.stream_logger
.. autofunction:: dkit.utilities.log_helper.stdout_logger
.. autofunction:: dkit.utilities.log_helper.stderr_logger

intervals
=========

.. automodule:: dkit.utilities.intervals
    :members:

numeric
=======
.. automodule:: dkit.utilities.numeric
    :members:

network_helper
==============
.. automodule:: dkit.utilities.network_helper
.. autofunction:: dkit.utilities.network_helper.download_file

time_helper
===========
.. automodule:: dkit.utilities.time_helper
    :members:

introspection
=============
.. automodule:: dkit.utilities.introspection

classes
-------

ClassDocumenter
~~~~~~~~~~~~~~~
.. image:: ../images/ClassDocumenter.svg
   :align: center

.. autoclass:: dkit.utilities.introspection.ClassDocumenter
   :members:
   :undoc-members:
   :inherited-members:

FunctionDocumenter
~~~~~~~~~~~~~~~~~~
.. image:: ../images/FunctionDocumenter.svg
   :align: center

.. autoclass:: dkit.utilities.introspection.FunctionDocumenter
   :members:
   :undoc-members:
   :inherited-members:

ModuleDocumenter
~~~~~~~~~~~~~~~~

.. image:: ../images/ModuleDocumenter.svg
   :align: center

.. autoclass:: dkit.utilities.introspection.ModuleDocumenter
   :members:
   :undoc-members:
   :inherited-members:


functions
---------
.. autofunction:: dkit.utilities.introspection.get_module_names
.. autofunction:: dkit.utilities.introspection.get_packages_names
.. autofunction:: dkit.utilities.introspection.get_class_names
.. autofunction:: dkit.utilities.introspection.get_function_names
.. autofunction:: dkit.utilities.introspection.get_property_names
.. autofunction:: dkit.utilities.introspection.get_method_names
.. autofunction:: dkit.utilities.introspection.get_routine_names
.. autofunction:: dkit.utilities.introspection.is_list


* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
