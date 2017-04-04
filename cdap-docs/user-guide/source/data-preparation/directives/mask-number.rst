.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

===========
Mask Number
===========

The MASK-NUMBER directive applies substitution masking on the column
values.

Syntax
------

::

    mask-number <columm> <pattern>

-  The ``<column>`` specifies the name of an existing column to be
   masked
-  The ``<pattern>`` is a substitution pattern to be used to mask the
   column values.

Usage Notes
-----------

Substitution masking is generally used for masking credit card or social
security numbers. The MASK-NUMBER applies substitution masking on the
column values. This type of masking is fixed masking, where the pattern
is applied on the fixed length string.

These rules are used for the pattern:

-  Use of ``#`` will include the digit from the position
-  Use ``x`` or any other character to mask the digit at that position

Examples
--------

Using this record as an example:

::

    {
      "first": "Root",
      "last": "Joltie",
      "ssn": "000-00-0000",
      "cc": "4929790943424701"
    }

Applying this directive:

::

    mask-number ssn XXX-XX-####

would result in this record:

::

    {
      "first": "Root",
      "last": "Joltie",
      "ssn": "XXX-XX-0000",
      "cc": "4929790943424701"
    }

Applying this directive:

::

    mask-number cc XXXXXXXXXXXX####

would result in this record:

::

    {
      "first": "Root",
      "last": "Joltie",
      "ssn": "000-00-0000",
      "cc": "XXXXXXXXXXXX4701"
    }
