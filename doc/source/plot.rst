****
plot
****

.. toctree::
   :maxdepth: 2

Package Overview
================

.. inheritance-diagram:: dkit.plot.ggrammar

.. automodule:: dkit.plot.ggrammar

Example Usage
=============

.. literalinclude:: ../../examples/example_barplot.py
   :language:  python 

Produces the following GnuPlot file:

.. literalinclude:: ../../examples/example_barplot.plot

And the following image:

.. image:: ../../examples/example_barplot.svg
   :align: center


Plot Objects
============
Plot Objects are specialized for specific data structures.

Plot
----

.. inheritance-diagram:: dkit.plot.ggrammar.Plot

.. autoclass:: dkit.plot.ggrammar.Plot
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members:

GeomHistogram
-------------
Example Usage:

.. literalinclude:: ../../examples/example_histplot.py
   :language: python

The above snippet will produce the following image:

.. image:: ../../examples/example_hist.svg

.. inheritance-diagram:: dkit.plot.ggrammar.GeomHistogram

.. autoclass:: dkit.plot.ggrammar.GeomHistogram
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members:

Modifiers
=========

Aestethic
---------

.. inheritance-diagram:: dkit.plot.ggrammar.Aesthetic

.. autoclass:: dkit.plot.ggrammar.Aesthetic
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members:

XAxis
-----

.. inheritance-diagram:: dkit.plot.ggrammar.XAxis

.. autoclass:: dkit.plot.ggrammar.XAxis
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members:

YAxis
-----

.. inheritance-diagram:: dkit.plot.ggrammar.YAxis

.. autoclass:: dkit.plot.ggrammar.YAxis
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members:

Title
-----

.. inheritance-diagram:: dkit.plot.ggrammar.Title

.. autoclass:: dkit.plot.ggrammar.Title
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members:

Plot Types
==========

AbstractGeom
------------

.. inheritance-diagram:: dkit.plot.ggrammar.AbstractGeom

.. autoclass:: dkit.plot.ggrammar.AbstractGeom
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members:

GeomArea
--------

.. inheritance-diagram:: dkit.plot.ggrammar.GeomArea

.. autoclass:: dkit.plot.ggrammar.GeomArea
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members:

GeomBar
-------

.. inheritance-diagram:: dkit.plot.ggrammar.GeomBar

.. autoclass:: dkit.plot.ggrammar.GeomBar
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members:

GeomLine
--------

.. inheritance-diagram:: dkit.plot.ggrammar.GeomLine

.. autoclass:: dkit.plot.ggrammar.GeomLine
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members:

GeomScatter
-----------

.. inheritance-diagram:: dkit.plot.ggrammar.GeomScatter

.. autoclass:: dkit.plot.ggrammar.GeomScatter
   :members:
   :undoc-members:
   :show-inheritance:

Backends
========

Backend
-------

.. inheritance-diagram:: dkit.plot.base.Backend

.. autoclass:: dkit.plot.base.Backend
   :members:
   :undoc-members:
   :show-inheritance:

BackendGnuPlot
--------------

.. inheritance-diagram:: dkit.plot.gnuplot.BackendGnuPlot

.. autoclass:: dkit.plot.gnuplot.BackendGnuPlot
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members:


* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
