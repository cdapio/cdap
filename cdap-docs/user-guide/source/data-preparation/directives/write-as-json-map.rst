.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

=================
Write as JSON Map
=================

The WRITE-AS-JSON-MAP directive converts the record into a JSON map.

Syntax
------

::

    write-as-json-map <column>

The ``<column>`` will contain a JSON map of all the fields in the
record.

Usage Notes
-----------

The WRITE-AS-JSON-MAP directive converts the entire record into a JSON
map. If the ``<column>`` already exists, it will overwrite it.

Depending on the type of object a field is holding, it will be
transformed appropriately.

Example
-------

Using this record as an example:

::

    {
      "int": 1,
      "string": "this, is a string."
    }

Applying this directive:

::

    write-as-json-map body

would result in this record:

::

    {
      "body": { "int":1, "string": "this, is a string." },
      "int": 1,
      "string": "this, is a string."
    }
