.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

====
Copy
====

The COPY directive copies values from a source column into a destination
column.

Syntax
------

::

    copy <source> <destination> [<force>]

The COPY directive copies data from the ``<source>`` column into the
``<destination>`` column.

If the ``<destination>`` column already exists, the ``<force>`` option
can be set to ``true`` to override any existing data in that column. By
default, the ``<force>`` option is set to ``false``.

Usage Notes
-----------

The COPY directive will copy data from ``<source>`` if and only if
``<source>`` column exists. If the ``<source>`` doesn't exist in the
record, the execution will fail with an error.

Copying makes a deep copy of the source into the destination. The type
of data from the source in the destination column is maintained as-is.

Example
-------

Using this record as an example:

::

    {
      "id": 1,
      "timestamp": 1234434343,
      "measurement": 10.45,
      "isvalid": true,
      "message": {
         "code": 132,
         "text": "Failure in the temperature sensor"
      }
    }

Applying these directives:

::

    copy timestamp datetime
    copy message status

would result in this record:

::

    {
      "id": 1,
      "timestamp": 1234434343,
      "datetime": 1234434343,
      "measurement": 10.45,
      "isvalid": true,
      "message": {
         "code": 132,
         "text": "Failure in the temperature sensor"
      },
      "status": {
          "code": 132,
          "text": "Failure in the temperature sensor"
      }
    }
