.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

====================
Write as JSON object
====================

The WRITE-AS-JSON-OBJECT directive composes a JSON object based on the
fields specified and writes it to the destination column.

Syntax
------

::

    write-as-json-object <destination-column> <source-column>[,<source-column>]*

The ``<destination-column>`` will contain a JSON object composed of all
the fields specified in the ``<source-column>``.

Usage Notes
-----------

The WRITE-AS-JSON-OBJECT directive composes a JSON Object based on the
fields or columns specified to be added to the object.

Depending on the type of object a field is holding, it will be
transformed appropriately to the JSON types. NULL are also handled and
converted to JsonNull.

Example
-------

Using this record as an example:

::

    {
      "number": 1,
      "text": "this, is a string.",
      "height" : 1.5,
      "weight" : 1.67,
      "address" : null
    }

And applying this directive:

::

    write-as-json-object body number,text,height,address

would result in this record:

::

    {
      "body": { "number":1, "text": "this, is a string.", "height" : 1.5, "address" : null },
      "number": 1,
      "text": "this, is a string.",
      "height" : 1.5,
      "weight" : 1.67,
      "address" : null
    }
