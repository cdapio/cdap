.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

=============
Cut Character
=============

The CUT-CHARACTER directive selects parts of a string value, accepting
standard `cut
options <http://man7.org/linux/man-pages/man1/cut.1.html>`__.

Syntax
------

::

    cut-character <source> <destination> <type> <range|indexes>

The ``<type> <range|indexes>`` are the standard `cut
options <http://man7.org/linux/man-pages/man1/cut.1.html>`__.

Usage Notes
-----------

The standard options for the CUT-CHARACTER directive include a list of
one or more ranges of characters. Each range is prefaced with a type
(``-b`` byte, ``-c`` character, ``-f`` field) and written as:

-  ``N`` The N'th byte, character, or field, counting from 1
-  ``N-`` From the N'th byte, character, or field, to the end of the
   line
-  ``N-M`` From the N'th to M'th (included) byte, character, or field
-  ``-M`` From the first to M'th (included) byte, character, or field

Example
-------

Using this record as an example:

::

    {
      "body": "one two three four five six seven eight"
    }

Applying these directives:

::

    cut-character body one -c 1-3
    cut-character body two -c 5-7
    cut-character body three -c 9-13
    cut-character body four -c 15-
    cut-character body five -c 1,2,3
    cut-character body six -c -3
    cut-character body seven -c 1,2,3-5

would result in this record:

::

    {
      "body": "one two three four five six seven eight",
      "one": "one",
      "two": "two",
      "three": "three",
      "four": "four five six seven eight",
      "five": "one",
      "six": "one",
      "seven": "one t"
    }
