*************************************
How to: ETL model & schema management
*************************************

The :doc:`tutorial` opened a source directly with a bare
``ModelManager.from_file(None)`` — an empty, in-memory model with no
connections or stored entities. This guide covers the rest of the
``ModelManager`` API: persisting a model to a YAML file, registering
database connections and endpoints, reading and writing through those
endpoints, and describing relationships between entities. It assumes
you have read the tutorial and are comfortable with ``Entity`` and
sources/sinks.

Everything shown here as Python has a ``dk`` CLI equivalent — see the
:doc:`cli` reference for ``dk connections``, ``dk endpoints``,
``dk schemas``, ``dk mapping``, and ``dk queries``, and
:doc:`howto_cli_exploration` for a worked example.

Creating and persisting a model
==================================

A model needs a configuration object before it can encrypt connection
passwords — even if you never store a password, ``ModelManager``
requires an encryption key to be configured. In a script:

.. code-block:: python

   import configparser
   from dkit.etl.model import ModelManager

   config = configparser.ConfigParser()
   config.read_dict({"DEFAULT": {"key": "<a Fernet key>"}})

   m = ModelManager.from_file(None, config=config)

From the CLI, ``dk admin init_config`` generates this configuration file
(with a real Fernet key) once per machine, and ``dk admin init_model``
creates an empty model file — you will not normally construct a
``ConfigParser`` by hand outside of a script.

A model is a :class:`dkit.data.map_db.FileObjectMapDB` — it can be saved
to and loaded from a YAML (or JSON, or pickle) file:

.. code-block:: python

   m.save("model.yml")

   m2 = ModelManager.from_file("model.yml", config=config)

The codec is chosen from the file extension (``.yml``/``.yaml`` → YAML,
``.json`` → JSON, ``.pickle`` → pickle).

Registering a connection
===========================

A :class:`~dkit.etl.model.Connection` describes how to reach a database.
``add_connection`` parses a SQLAlchemy-style URI and stores it on the
model, encrypting the password if one is present:

.. code-block:: python

   m.add_connection("titanic_db", "sqlite:////tmp/titanic.db")

Equivalent CLI command:

.. code-block:: bash

   dk connections add -c titanic_db sqlite:////tmp/titanic.db

Registering an endpoint
==========================

An :class:`~dkit.etl.model.Endpoint` names a specific table (or other
addressable location) on a connection, and associates it with a stored
``Entity`` schema:

.. code-block:: python

   from dkit.etl.model import Entity

   with m.source("examples/data/titanic.csv") as src:
       entity = Entity.from_iterable(src, infer_strings=True)
   m.entities["passenger"] = entity

   m.add_endpoint(
       "passenger_table",
       connection="titanic_db",
       table="passenger",
       entity="passenger",
   )

Equivalent CLI command:

.. code-block:: bash

   dk endpoints add -E passenger_table -c titanic_db -e passenger -b passenger

Reading and writing through an endpoint
==========================================

Once an endpoint is registered, ``::endpoint_name`` can be used anywhere
a source or sink URI is expected, instead of a raw connection string.
The endpoint's connection and table name are resolved from the model:

.. code-block:: python

   from dkit.etl.extensions.ext_sql_alchemy import SQLAlchemyAccessor

   # create the destination table from the entity's schema, once
   conn_dict = m.get_connection("titanic_db").as_dict(include_none=True)
   accessor = SQLAlchemyAccessor(conn_dict)
   accessor.create_table("passenger", entity.as_entity_validator())

   # write through the endpoint
   with m.source("examples/data/titanic.csv") as src:
       with m.sink("::passenger_table") as snk:
           snk.process(entity(src))

   # read it back through the same endpoint
   with m.source("::passenger_table") as src:
       rows = list(src)

Because the endpoint carries both the connection and the schema, code
that reads or writes ``::passenger_table`` does not need to know it is
backed by SQLite specifically — pointing the same endpoint at a
different connection (Postgres, for example) requires no changes to the
processing code.

Describing relations between entities
========================================

:class:`~dkit.etl.model.Relation` records a foreign-key-style
relationship between two entities already registered on the model.
``add_relation`` validates that the referenced columns actually exist on
both entities before storing the relation:

.. code-block:: python

   m.add_relation(
       "passenger_ticket",
       const_entity="passenger",
       ref_entity="ticket",
       const_cols=["Ticket"],
       ref_cols=["Ticket"],
   )

This is metadata only — dkit does not enforce the relation at write
time — but it is used by schema export (for example, to GraphViz
entity-relation diagrams) and is a convenient place to record how the
entities in a model fit together.

Equivalent CLI command:

.. code-block:: bash

   dk mapping add -r passenger_ticket -M passenger -O ticket --mc Ticket --oc Ticket

Storing reusable queries
===========================

A :class:`~dkit.etl.model.Query` stores a Jinja2-templated SQL string on
the model, so a parameterised query can be reused without editing a
script each time:

.. code-block:: python

   from dkit.etl.model import Query

   m.queries["over_18"] = Query(
       query="select * from passenger where Age > {{ min_age }}",
       description="passengers older than a given age",
   )

   rendered = m.queries["over_18"](min_age=18)

Equivalent CLI command, running a stored query with the ``run query``
subcommand and a parameter:

.. code-block:: bash

   dk run query -c titanic_db -q over_18 -p min_age=18

Secrets
=========

``add_secret`` and ``get_secret`` store and retrieve arbitrary encrypted
key/secret pairs on the model (for example, API credentials that are not
a database connection), using the same Fernet key as connection
passwords:

.. code-block:: python

   m.add_secret("api_token", key="client_id_123", secret="super-secret-value")
   secret = m.get_secret("api_token")

The ``dk vault`` module manages secrets from the command line; see the
:doc:`cli` reference.

Where to go next
===================

- :doc:`howto_documents` — generate a report or email from data read
  through a model.
- :doc:`howto_cli_exploration` — the same connections, endpoints, and
  queries used above, driven entirely from ``dk``.
- :doc:`etl` — full API reference for ``ModelManager`` and the classes
  used in this guide.
