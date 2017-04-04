.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.
    
==============================
Increment a transient variable
==============================

The INCREMENT-VARIABLE directive increments the value of the variable
that is local to the input record being processed.

Syntax
------

::

    increment-variable <variable> <value> <expression>

The ``<variable>`` is incremented by the ``<value>`` when the
``<expression>`` evaluates to true.

Usage Notes
-----------

This directive is applied only within the scope of the record being
processed. The transient state is reset, once the system starts
processing of the new record.

Internally, this directive increments a ``long`` value.
