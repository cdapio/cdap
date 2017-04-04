.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

==================
Change Column Case
==================

The CHANGE-COLUMN-CASE directive changes column names to either
lowercase or uppercase.

Syntax
------

::

    change-column-case lower|upper

If the case specified is either incorrect or missing, it defaults to
lowercase.

Examples
--------

Using this record as an example:

::

    {
      "Id": 1,
      "Gender": "male",
      "FNAME": "Root",
      "lname": "JOLTIE",
      "Address": "67 MARS AVE, MARSCIty, Marsville, Mars"
    }

Applying this directive:

::

    change-column-case lower

would result in this record:

::

    {
      "id": 1,
      "gender": "MALE",
      "fname": "Root",
      "lname": "Joltie",
      "address": "67 mars ave, marscity, marsville, mars"
    }

Applying this directive:

::

    change-column-case upper

would result in this record:

::

    {
      "ID": 1,
      "GENDER": "MALE",
      "FNAME": "Root",
      "LNAME": "Joltie",
      "ADDRESS": "67 mars ave, marscity, marsville, mars"
    }
