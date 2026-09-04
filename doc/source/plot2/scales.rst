******
Scales
******

A scale owns one axis: its type, label, limits, tick locations and tick
formats. ``Plot`` requires them to be named; the ``quick`` functions infer
them from the data with :func:`dkit.plot2.scale.infer`. See
:ref:`example-usage-declarative-plot` for scales in use.

.. automodule:: dkit.plot2.scale

.. inheritance-diagram:: dkit.plot2.scale.Categorical dkit.plot2.scale.Linear
                         dkit.plot2.scale.Log dkit.plot2.scale.Percent
                         dkit.plot2.scale.Time

.. autoclass:: dkit.plot2.scale.Scale
   :members:

.. autoclass:: dkit.plot2.scale.Linear
   :members:
   :show-inheritance:

.. autoclass:: dkit.plot2.scale.Categorical
   :members:
   :show-inheritance:

.. autoclass:: dkit.plot2.scale.Time
   :members:
   :show-inheritance:

.. autoclass:: dkit.plot2.scale.Percent
   :members:
   :show-inheritance:

.. autoclass:: dkit.plot2.scale.Log
   :members:
   :show-inheritance:

.. autofunction:: dkit.plot2.scale.infer
