.. _overview_toplevel:

========
Overview
========

.. _overview:

Overview
========

The SQLAlchemy SQL Toolkit and Object Relational Mapper
is a comprehensive set of tools for working with
databases and Python. It has several distinct areas of
functionality which can be used individually or combined
together. Its major components are illustrated in below,
with component dependencies organized into layers:

.. image:: sqla_arch_small.png

Above, the two most significant front-facing portions of
SQLAlchemy are the **Object Relational Mapper** and the
**SQL Expression Language**. SQL Expressions can be used
independently of the ORM. When using the ORM, the SQL
Expression language remains part of the public facing API
as it is used within object-relational configurations and
queries.

.. _doc_overview:

Documentation Overview
======================

The documentation is separated into three sections: :ref:`orm_toplevel`,
:ref:`core_toplevel`, and :ref:`dialect_toplevel`.

In :ref:`orm_toplevel`, the Object Relational Mapper is introduced and fully
described. New users should begin with the :ref:`ormtutorial_toplevel`. If you
want to work with higher-level SQL which is constructed automatically for you,
as well as management of Python objects, proceed to this tutorial.

In :ref:`core_toplevel`, the breadth of SQLAlchemy's SQL and database
integration and description services are documented, the core of which is the
SQL Expression language. The SQL Expression Language is a toolkit all its own,
independent of the ORM package, which can be used to construct manipulable SQL
expressions which can be programmatically constructed, modified, and executed,
returning cursor-like result sets. In contrast to the ORM's domain-centric
mode of usage, the expression language provides a schema-centric usage
paradigm. New users should begin here with :ref:`sqlexpression_toplevel`.
SQLAlchemy engine, connection, and pooling services are also described in
:ref:`core_toplevel`.

In :ref:`dialect_toplevel`, reference documentation for all provided
database and DBAPI backends is provided.

.. _installation:

Installation Guide
==================

Supported Platforms
-------------------

SQLAlchemy has been tested against the following platforms:

* cPython since version 2.6, through the 2.xx series
* cPython version 3, throughout all 3.xx series
* `Pypy <http://pypy.org/>`_ 2.1 or greater

.. versionchanged:: 0.9
   Python 2.6 is now the minimum Python version supported.


