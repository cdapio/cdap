.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

==========
Set Column
==========

The SET-COLUMN directive sets the column value to the result of an
expression execution.

Syntax
------

::

    set-column <columm> <expression>

-  The ``<column>`` specifies the name of a column. If the column exists
   already, its value will be overwritten with the result of the
   specified expression. If the column does not exist, a new column will
   be created with the result of the specified expression.
-  The ``<expression>`` is a valid `Apache Commons JEXL
   expression <http://commons.apache.org/proper/commons-jexl/reference/syntax.html>`__

Usage Notes
-----------

The SET-COLUMN directive sets the column value to the result of the
execution of an expression.

Expressions are written in `Apache Commons
JEXL <http://commons.apache.org/proper/commons-jexl/reference/syntax.html>`__
notation.

Functions from other namespaces (such as ``string`` and ``math``) can be
called by adding the namespace and a colon before the function, such as
``math:ceil`` or ``string:upperCase``.

Examples
--------

Using this record as an example:

::

    {
      "first": "Root",
      "last": "Joltie",
      "age": "32",
      "hrlywage": "11.79",
    }

Applying these directives:

::

    set-column name concat(last, ", ", first)
    set-column is_adult age > 21 ? 'yes' : 'no'
    set-column salary hrlywage*40*50
    set column raised_hrlywage var x; x = math:ceil(FLOAT(hrlywage)); x + 1
    set column salutation string:upperCase(concat(first, ' ', last))

would result in this record:

::

    {
      "first": "Root",
      "last": "Joltie",
      "age": "32",
      "hrlywage": "11.79",
      "name": "Joltie, Root",
      "is_adult": "yes",
      "salary": 23580.0,
      "raised_hrlywage": 13.0,
      "salutation": "ROOT JOLTIE"
    }
