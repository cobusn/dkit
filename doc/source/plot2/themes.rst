******
Themes
******

Styling is split in two. Anything matplotlib can already express lives in a
native ``.mplstyle`` file, validated by matplotlib itself;
:class:`~dkit.plot2.theme.Theme` adds only what rcParams cannot express --
colour maps, semantic colours and default number and date formats. See
:doc:`../howto_plot2_styles` for how to author a style sheet, and
:ref:`example-usage-themes` for runnable examples.

.. automodule:: dkit.plot2.theme

Theme
=====

.. autoclass:: dkit.plot2.theme.Theme
   :members:

The registry
============

.. autodata:: dkit.plot2.theme.themes
   :no-value:

.. autodata:: dkit.plot2.theme.DEFAULT_THEME

.. autofunction:: dkit.plot2.theme.get_theme

.. autofunction:: dkit.plot2.theme.register_theme

.. autofunction:: dkit.plot2.theme.unregister_theme

.. autofunction:: dkit.plot2.theme.set_default_theme

.. autofunction:: dkit.plot2.theme.default_theme

.. autofunction:: dkit.plot2.theme.bundled_styles

.. autofunction:: dkit.plot2.theme.resolve_style

.. autodata:: dkit.plot2.theme.CM_TO_INCH
   :no-value:
