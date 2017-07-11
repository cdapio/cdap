.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

===============
Columns Replace
===============

The COLUMNS-REPLACE directive alters column names in bulk.

Syntax
------

::

    columns-replace <sed-expression>

The ``<sed-expression>`` specifies the `sed
expression <https://www.gnu.org/software/sed/manual/html_node/Regular-Expressions.html>`__
syntax, such as ``s/data_//g``.

Example
-------

Using this record as an example, these columns all have ``data_`` as a
prefix in their column names:

::

    {
      "data_name": "root",
      "data_first_name": "mars",
      "data_last_name": "joltie",
      "data_data_id": 1,
      "data_address": "150 Mars Ave, Mars City, Mars, 8899898",
      "mars_ssn": "MARS-456282"
    }

Applying this directive:

::

    columns-replace s/^data_//g

would result in the record having any column names that were prefixed
with ``data_`` replaced with an empty string:

::

    {
      "name": "root",
      "first_name": "mars",
      "last_name": "joltie",
      "data_id": 1,
      "address": "150 Mars Ave, Mars City, Mars, 8899898",
      "mars_ssn": "MARS-456282"
    }

**Note:** The field value is untouched during this directive. This
operates only on the column names.
