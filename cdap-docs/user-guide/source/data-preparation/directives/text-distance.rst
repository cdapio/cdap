.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

=============
Text Distance
=============

The TEXT-DISTANCE directive measures the difference between two
sequences of characters, using a specified method of measuring the
distance between strings.

Syntax
------

::

    text-distance <method> <column-1> <column-2> <destination>

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
      "tweet2": "Forgiveness can open up a whole new perspective on life, Forgiveness is an act of God's grace."
    }

Applying this directive:

::

    text-distance block tweet1 tweet2 distance

would result in this record:

::

    {
      "tweet1": "CheeseBurgers are God tellin us everything's gonna be cool...",
      "tweet2": "Forgiveness can open up a whole new perspective on life, Forgiveness is an act of God's grace."
      "destination": 26.0
    }
