.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

==================
Filter Row If True
==================

The FILTER-ROW-IF-TRUE directive filters records that match a condition.

Deprecated
----------

Use the `FILTER-ROWS-ON <filter-rows-on.md>`__ directive instead.

Syntax
------

::

    filter-row-if-true <condition>

The ``<condition>`` is a valid boolean expression resulting in either a
``true`` or ``false``.

Usage Notes
-----------

The FILTER-ROW-IF-TRUE directive evaluates the Boolean condition for
each record. If the result of evaluation is ``true``, it skips the
record, otherwise it is passed as-is to the input of the next directive.

The ``<condition>`` is specified in the JEXL expression language with
additional utility libraries are defined in the ``math`` and ``string``
namespaces.

For information on how to write JEXL expressions, see the `commons-jexl
documentation <https://commons.apache.org/proper/commons-jexl/reference/syntax.html>`__.

Examples
--------

Using this record as an example:

::

    {
      "id": 1,
      "name": "Joltie, Root",
      "hrlywage": "12.34",
      "gender": "Male",
      "country": "US"
    }

Applying this directive:

::

    filter-row-if-true country !~ US

would result in filtering out records for individuals that are not in
the US (where ``country`` does not match "US").

Applying this directive:

::

    filter-row-if-true (country !~ US && hrlywage > 12)

would result in filtering out records for individuals that are not in
the US (where ``country`` does not match "US") and whose hourly wage
(``hrlywage``) is greater than 12.
