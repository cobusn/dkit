*************
CLI reference
*************

The ``dk`` command exposes most of the library's functionality directly
from the shell, without writing a script. It is invoked as:

.. code-block:: text

   dk MODULE [subcommand] [options]

Run ``dk MODULE -h`` for the full option list of any module. The
subcommand references below are generated directly from each module's
``argparse`` definitions, so they stay in sync with the actual CLI as it
evolves.

.. note::
   Set the ``DK_DEBUG`` environment variable to ``True`` to disable
   ``dk``'s exception handling and see full Python tracebacks — useful
   when scripting against ``dk`` or reporting a bug.

Maintenance modules
====================

These modules manage the persistent state of an ETL project — the model
file's connections, endpoints, queries, schemas, transforms, relations,
and configuration.

admin
-----

.. argparse::
   :ref: _ext.dk_parsers.admin_parser

connections
------------

.. argparse::
   :ref: _ext.dk_parsers.connections_parser

endpoints
----------

.. argparse::
   :ref: _ext.dk_parsers.endpoints_parser

mapping
--------

.. argparse::
   :ref: _ext.dk_parsers.mapping_parser

queries
--------

.. argparse::
   :ref: _ext.dk_parsers.queries_parser

schemas
--------

.. argparse::
   :ref: _ext.dk_parsers.schemas_parser

transforms
-----------

.. argparse::
   :ref: _ext.dk_parsers.transforms_parser

vault
------

Manage encrypted secrets (also referred to as the "vault") used by
connections and endpoints.

.. argparse::
   :ref: _ext.dk_parsers.vault_parser

XML
----

.. argparse::
   :ref: _ext.dk_parsers.xml_parser

Action modules
================

These modules perform the actual data-processing work: running ETL
pipelines and queries, exploring data, diffing datasets, and building
documents.

build
------

.. argparse::
   :ref: _ext.dk_parsers.build_parser

diff
-----

.. argparse::
   :ref: _ext.dk_parsers.diff_parser

run
----

.. argparse::
   :ref: _ext.dk_parsers.run_parser

xplore
-------

.. argparse::
   :ref: _ext.dk_parsers.xplore_parser
