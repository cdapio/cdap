.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

===========
Text Metric
===========

The TEXT-METRIC directive provides a metric (from 0 to 1) measuring the
difference between two sequence of characters, using a specified method
of measuring the distance between strings.

Syntax
------

::

    text-metric <method> <column-1> <column-2> <destination>

-  ``<method>`` specifies the method to be used to measure the distance
   between the strings of ``<column-1>`` and ``<column-2>``
-  ``<destination>`` is the column the resulting difference will be
   stored in. If it exists, it will be overwritten. If it does not
   exist, it will be created.

**Note:** If either or both of the two columns do not exist, no error
will be returned, and the destination column will still be created or
overwritten.

Usage Notes
-----------

The value of the metric is always between 0 and 1.

These distance measure methods are supported:

-  ``block-distance``
-  ``block``
-  ``cosine``
-  ``damerau-levenshtein``
-  ``dice``
-  ``euclidean``
-  ``generalized-jaccard``
-  ``identity``
-  ``jaccard``
-  ``jaro``
-  ``levenshtein``
-  ``longest-common-subsequence``
-  ``longest-common-substring``
-  ``overlap-cofficient``
-  ``simon-white``

Example
-------

Using this record as an example:

::

    {
      "tweet1": "CheeseBurgers are God tellin us everything's gonna be cool...",
      "tweet2": "Beer is God's gift to humankind."
    }

Applying this directive:

::

    text-metric longest-common-subsequence tweet1 tweet2 distance

would result in this record:

::

    {
      "tweet1": "CheeseBurgers are God tellin us everything's gonna be cool...",
      "tweet2": "Beer is God's gift to humankind."
      "destination": 0.26229507
    }
