:orphan:

.. _faq_toplevel:

============================
Frequently Asked Questions
============================

.. contents::
        :local:
        :class: faq
        :backlinks: none


Connections / Engines
=====================

How do I configure logging?
---------------------------

See :ref:`dbengine_logging`.

How do I pool database connections?   Are my connections pooled?
----------------------------------------------------------------

SQLAlchemy performs application-level connection pooling automatically
in most cases.  With the exception of SQLite, a :class:`.Engine` object
refers to a :class:`.QueuePool` as a source of connectivity.

For more detail, see :ref:`engines_toplevel` and :ref:`pooling_toplevel`.

How do I pass custom connect arguments to my database API?
-----------------------------------------------------------

The :func:`.create_engine` call accepts additional arguments either
directly via the ``connect_args`` keyword argument::

        e = create_engine("mysql://scott:tiger@localhost/test",
                                                connect_args={"encoding": "utf8"})

Or for basic string and integer arguments, they can usually be specified
in the query string of the URL::

        e = create_engine("mysql://scott:tiger@localhost/test?encoding=utf8")

.. seealso::

        :ref:`custom_dbapi_args`


