.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

============
Mask Shuffle
============

The MASK-SHUFFLE directive applies shuffle masking on the column values.

Syntax
------

::

    mask-shuffle <columm>

The ``<column>`` specifies the name of an existing column to be masked.

Usage Notes
-----------

The MASK-SHUFFLE directive applies shuffle masking on the column values
to perform obfuscation by using random character substitution method.
This type of masking is fixed length masking, where the data is randomly
shuffled in the column on the fixed length string.

Note that vowels, consonants, and numerics are each shuffled separately
and retain their case.

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

Applying these directives:

::

    mask-shuffle first
    mask-shuffle last
    mask-shuffle ssn
    mask-shuffle cc

would result in this record:

::

    {
      "first": "Buek",
      "last": "Bumkyy",
      "ssn": "089-75-3119",
      "cc": "0897531194773254"
    }
