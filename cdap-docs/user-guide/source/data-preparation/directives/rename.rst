.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

======
Rename
======

The RENAME directive renames an existing column in the record.

Syntax
------

::

    rename <old> <new>

-  ``<old>`` is the name of an existing column to be renamed
-  ``<new>`` is the new name of the column

Usage Notes
-----------

The RENAME directive will rename the specified column name by replacing
it with a new name. The original column name will no longer be available
in the record after this directive has been applied to the record.

The RENAME directive will only rename a column that exists. If the
column name does not exist in the record, the operation will be ignored
without an error.

Example
-------

Using this record as an example:

::

    {
      "x": 6.3,
      "y": 187,
      "codes": {
        "a": "code1",
        "b": 2
      }
    }

Applying these directives:

::

    rename x height
    rename y weight

would result in this record:

::

    {
      "height": 6.3,
      "weight": 187,
      "codes": {
        "a": "code1",
        "b": 2
      }
    }
