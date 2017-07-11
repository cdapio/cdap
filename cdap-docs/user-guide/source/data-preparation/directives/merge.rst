.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

=====
Merge
=====

The MERGE directive merges two columns by inserting a third column into
a record. The values in the third column are merged values from the two
columns delimited by a specified separator.

Syntax
------

::

     merge <first> <second> <new column> '<seperator>'

-  The ``<first>`` and ``<second>`` column values are merged using a
   separator. The columns to be merged must both exist and should be of
   type string for the merge to be successful.

-  The ``<new-column>`` is the new column that will be added to the
   record. If the column already exists, the contents will be replaced.

-  The ``<separator>`` is the character or string to be used to separate
   the values in the new column. It is specified between single quotes.
   For example, a space character: ``' '``.

-  The existing columns are not dropped by this directive.

Usage Notes
-----------

The columns to be merged should both be of type string.

Examples
--------

Using this record as an example:

::

    {
      "first": "Root",
      "last": "Joltie"
    }

Applying these directives:

::

    merge first last fullname ' '
    merge first last fullname '''
    merge first last fullname '\u000A'
    merge first last fullname '---'

would result in these records:

Separator is a single space character (``' '``):

::

    {
      "first": "Root",
      "last": "Joltie",
      "fullname": "Root Joltie"
    }

Separator is a single quote character (``'''``):

::

    {
      "fname" : "Joltie",
      "lname" : "Root",
      "name" : "Joltie'Root"
    }

Separator is the UTF-8 Line Feed character (``'\u000A'``):

::

    {
      "fname" : "Joltie",
      "lname" : "Root",
      "name" : "Joltie\nRoot"
    }

Separator is multiple characters (``'---'``):

::

    {
      "fname" : "Joltie",
      "lname" : "Root",
      "name" : "Joltie---Root"
    }
