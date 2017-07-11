.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

=======
Flatten
=======

The FLATTEN directive separates the elements in a repeated field into
individual records.

Syntax
------

::

    flatten <column>[,<column>]*

The ``<column>`` is the name of a column that is a JSON array.

Usage Notes
-----------

The FLATTEN directive is useful for the flexible exploration of repeated
data.

To maintain the association between each flattened value and the other
fields in the record, the FLATTEN directive copies all of the other
columns into each new record.

Examples
--------

Case 1
~~~~~~

The array in ``col2`` is flattened and the values in ``col3`` are
repeated for each value of ``col2``:

**Input Record**

::

    [
      { "col1": "A" },
      { "col1": "B" },
      { "col2": [x1, y1], "col3": 10 },
      { "col2": [x2, y2], "col3": 11 }
    ]

**Output Record**

::

    [
      { "col1": "A" },
      { "col1": "B" },
      { "col2": "x1", "col3": 10 },
      { "col2": "y1", "col3": 10 },
      { "col2": "x2", "col3": 11 },
      { "col2": "y2", "col3": 11 }
    ]

Case 2
~~~~~~

The arrays in ``col2`` and ``col3`` are flattened:

**Input Record**

::

    [
      { "col1": "A" },
      { "col1": "B" },
      { "col2": [ "x1", "y1", "z1" ], "col3": [ "a1", "b1", "c1" ] },
      { "col2": [ "x2", "y2" ], "col3": [ "a2", "b2" ] }
    ]

**Output Record**

::

    [
      { "col1": "A" },
      { "col1": "B" },
      { "col2": "x1", "col3": "a1" },
      { "col2": "y1", "col3": "b1" },
      { "col2": "z1", "col3": "c1" },
      { "col2": "x2", "col3": "a2" },
      { "col2": "y2", "col3": "b2" }
    ]

Case 3
~~~~~~

The arrays in ``col2`` and ``col3`` are flattened:

**Input Record**

::

    [
      { "col1": "A" },
      { "col1": "B" },
      { "col2": [ "x1", "y1", "z1" ], "col3": [ "a1", "b1" ] },
      { "col2": [ "x2", "y2" ], "col3": [ "a2", "b2", "c2" ] }
    ]

**Output Record**

::

    [
      { "col1": "A" },
      { "col1": "B" },
      { "col2": "x1", "col3": "a1" },
      { "col2": "y1", "col3": "b1" },
      { "col2": "z1" },
      { "col2": "x2", "col3": "a2" },
      { "col2": "y2", "col3": "b2" },
      { "col3": "c2" }
    ]

Case 4
~~~~~~

Using this record as an example:

::

    {
      "x": 5,
      "y": "a string",
      "z": [ 1,2,3 ]
    }

The directive would result in these three distinct records:

+-----+--------------+-----+
| x   | y            | z   |
+=====+==============+=====+
| 5   | "a string"   | 1   |
+-----+--------------+-----+
| 5   | "a string"   | 2   |
+-----+--------------+-----+
| 5   | "a string"   | 3   |
+-----+--------------+-----+

The directive takes a single argument, which must be an array (the ``z``
column in this example). In this case, using the "all" (``*``) wildcard
as the argument to flatten is not supported and would return an error.
