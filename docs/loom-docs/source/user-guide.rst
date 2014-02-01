.. _overview_toplevel:

========
User Guide
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

Administrator Setup
===================

Loom requires configuration by an experienced
administrator. The admin will be responsible for
configuring Loom's hardware, software, and access controls.

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


