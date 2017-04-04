.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

====
Swap
====

The SWAP directive swaps column names of two columns.

Syntax
------

::

    swap <column1> <colum2>

Usage Notes
-----------

The SWAP directive renames ``<column1>`` to the name of ``<column2>``
and ``<column2>`` to the name of ``<column1>``. If the either of the two
columns are not present, execution of the directive fails.

Example
-------

Using this record as an example:

::

    {
      "a": 1,
      "b": "sample string"
    }

Applying either of these directives:

::

    swap a b
    swap b a

would result in this record:

::

    {
      "b": 1,
      "a": "sample string"
    }
