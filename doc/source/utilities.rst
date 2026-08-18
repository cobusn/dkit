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
.. inheritance-diagram:: dkit.utilities.instrumentation.Counter

Members
~~~~~~~
.. autoclass:: dkit.utilities.instrumentation.Counter
   :members:
   :undoc-members:

CounterLogger
-------------

Class Diagram
~~~~~~~~~~~~~
.. inheritance-diagram:: dkit.utilities.instrumentation.CounterLogger

Members
~~~~~~~
.. autoclass:: dkit.utilities.instrumentation.CounterLogger
   :members:
   :undoc-members:

Exceptions
~~~~~~~~~~
.. inheritance-diagram:: dkit.utilities.instrumentation.TimerException

.. autoclass:: dkit.utilities.instrumentation.TimerException

Timer
-----

.. inheritance-diagram:: dkit.utilities.instrumentation.Timer

.. autoclass:: dkit.utilities.instrumentation.Timer
   :members:
   :undoc-members:

security
========
.. automodule:: dkit.utilities.security

Fernet
-------

.. inheritance-diagram:: dkit.utilities.security.Fernet

.. autoclass:: dkit.utilities.security.Fernet
   :members:
   :undoc-members:
   :show-inheritance:

Vigenere
--------

.. inheritance-diagram:: dkit.utilities.security.Vigenere

.. autoclass:: dkit.utilities.security.Vigenere
   :members:
   :undoc-members:
   :show-inheritance:

Pie
---

.. inheritance-diagram:: dkit.utilities.security.Pie

.. autoclass:: dkit.utilities.security.Pie
   :members:
   :undoc-members:
   :show-inheritance:

log_helper
==========
.. automodule:: dkit.utilities.log_helper
   :members:
   :undoc-members:


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
.. inheritance-diagram:: dkit.utilities.introspection.ClassDocumenter

.. autoclass:: dkit.utilities.introspection.ClassDocumenter
   :members:
   :undoc-members:
   :inherited-members:

FunctionDocumenter
~~~~~~~~~~~~~~~~~~
.. inheritance-diagram:: dkit.utilities.introspection.FunctionDocumenter

.. autoclass:: dkit.utilities.introspection.FunctionDocumenter
   :members:
   :undoc-members:
   :inherited-members:

ModuleDocumenter
~~~~~~~~~~~~~~~~

.. inheritance-diagram:: dkit.utilities.introspection.ModuleDocumenter

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

job_tracker
===========
.. automodule:: dkit.utilities.job_tracker

JobTracker
----------
.. inheritance-diagram:: dkit.utilities.job_tracker.JobTracker
   :private-bases:

.. autoclass:: dkit.utilities.job_tracker.JobTracker
   :members:
   :undoc-members:
   :inherited-members:

MultiProcessJobTracker
-----------------------
.. inheritance-diagram:: dkit.utilities.job_tracker.MultiProcessJobTracker
   :private-bases:

.. autoclass:: dkit.utilities.job_tracker.MultiProcessJobTracker
   :members:
   :undoc-members:
   :inherited-members:

This example illustrates use of JobTracker:

    .. include:: ../../examples/example_job_tracker.py
        :literal:


* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
