****
plot
****

.. toctree::
   :maxdepth: 2

Package Overview
================

.. image:: ../images/classes_plot.svg
	:align: center

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

.. image:: ../images/Plot.svg
	:align: center

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

.. image:: ../images/GeomHistogram.svg
	:align: center

.. autoclass:: dkit.plot.ggrammar.GeomHistogram
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members:

Modifiers
=========

Aestethic
---------

.. image:: ../images/Aesthetic.svg
	:align: center

.. autoclass:: dkit.plot.ggrammar.Aesthetic
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members:

XAxis
-----

.. image:: ../images/XAxis.svg
	:align: center

.. autoclass:: dkit.plot.ggrammar.XAxis
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members:

YAxis
-----

.. image:: ../images/YAxis.svg
	:align: center

.. autoclass:: dkit.plot.ggrammar.YAxis
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members:

Title
-----

.. image:: ../images/Title.svg
	:align: center

.. autoclass:: dkit.plot.ggrammar.Title
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members:

Plot Types
==========

AbstractGeom
------------

.. image:: ../images/AbstractGeom.svg
	:align: center

.. autoclass:: dkit.plot.ggrammar.AbstractGeom
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members:

GeomArea
--------

.. image:: ../images/GeomArea.svg
	:align: center

.. autoclass:: dkit.plot.ggrammar.GeomArea
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members:

GeomBar
-------

.. image:: ../images/GeomBar.svg
	:align: center

.. autoclass:: dkit.plot.ggrammar.GeomBar
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members:

GeomLine
--------

.. image:: ../images/GeomLine.svg
	:align: center

.. autoclass:: dkit.plot.ggrammar.GeomLine
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members:

GeomScatter
-----------

.. image:: ../images/GeomScatter.svg
	:align: center

.. autoclass:: dkit.plot.ggrammar.GeomScatter
   :members:
   :undoc-members:
   :show-inheritance:

Backends
========

Backend
-------

.. autoclass:: dkit.plot.base.Backend
   :members:
   :undoc-members:
   :show-inheritance:

BackendGnuPlot
--------------

.. image:: ../images/BackendGnuPlot.svg
	:align: center

.. autoclass:: dkit.plot.gnuplot.BackendGnuPlot
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members:


* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
