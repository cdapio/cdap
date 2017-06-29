.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

========================
Set a transient variable
========================

The SET-VARIABLE directive increments the value of the variable that is
local to the input record being processed.

Syntax
------

::

    set-variable <variable> <expression>

The ``<variable>`` is set the result of ``<expression>``.

Usage Notes
-----------

This directive is applied only within the scope of the record being
processed. The transient state is reset, once the system starts
processing of the new record.
