*****
Geoms
*****

.. automodule:: dkit.plot2.geom

Every geom below documents only what it adds: the arguments they all share --
``label``, ``x``, ``y``, ``color``, ``alpha``, ``where`` and ``axis`` -- are
on ``Geom``. TreeMap, Slope and CalendarHeatmap, which own their whole Axes
rather than drawing onto a pair of scales, are documented under
:doc:`extensions`.

.. autoclass:: dkit.plot2.geom.base.Geom
   :members:

.. autoclass:: dkit.plot2.geom.line.Line
   :members:

.. autoclass:: dkit.plot2.geom.bar.Bar
   :members:

.. autoclass:: dkit.plot2.geom.point.Scatter
   :members:

.. autoclass:: dkit.plot2.geom.line.Area
   :members:

.. autoclass:: dkit.plot2.geom.line.Band
   :members:

.. autoclass:: dkit.plot2.geom.line.Stem
   :members:

.. autoclass:: dkit.plot2.geom.matrix.HeatMap
   :members:

.. autoclass:: dkit.plot2.geom.reference.HLine
   :members:

.. autoclass:: dkit.plot2.geom.reference.VLine
   :members:

.. autoclass:: dkit.plot2.geom.reference.Text
   :members:

See :ref:`example-usage-geoms` for runnable examples.
