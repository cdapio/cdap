.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

=================
Fail on condition
=================

The FAIL directive will fail processing of pipeline when condition is
evaulated to ``true``.

Syntax
------

::

    fail <condition>

The ``<condition>`` is a JEXL expression specifing the condition that
governs if the record should be sent to the error collector.

Usage Notes
-----------

The most common use of the FAIL directive is to fail further processing
of the pipeline when the condition is met.

Example
-------

Assume a record that has these three fields:

-  Error Name
-  Error Count

These directives will implement these rules; any records that match any
of these conditions will terminate further processing of the pipeline.

::

    fail ErrorCount > 1
