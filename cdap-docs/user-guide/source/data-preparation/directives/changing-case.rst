.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

=============
Changing Case
=============

The UPPERCASE, LOWERCASE, and TITLECASE directives change the case of
column values they are applied to.

Syntax
------

::

    lowercase <column>
    uppercase <column>
    titlecase <column>

The directive performs an in-place change of case.

Example
-------

Using this record as an example:

::

    {
      "id": 1,
      "gender": "male",
      "fname": "Root",
      "lname": "JOLTIE",
      "address": "67 MARS AVE, MARSCIty, Marsville, Mars"
    }

Applying these directives

::

    uppercase gender
    titlecase fname
    titlecase lname
    lowercase address

would result in this record:

::

    {
      "id": 1,
      "gender": "MALE",
      "fname": "Root",
      "lname": "Joltie",
      "address": "67 mars ave, marscity, marsville, mars"
    }
