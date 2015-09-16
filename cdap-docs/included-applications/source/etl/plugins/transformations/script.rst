.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

=======================
Transformations: Script 
=======================

.. rubric:: Description

Executes user-provided Javascript that transforms one record into another.
Input records are converted into JSON objects which can be directly accessed in
Javascript. The transform expects to receive a JSON object as input, which it will
convert back to a record in Java to pass to downstream transforms and sinks. 

.. rubric:: Use Case

The Script transform is used when other transforms cannot meet your needs.
For example, you may want to multiply a field by 1024 and rename it from 'gigabytes'
to 'megabytes'. Or you might want to convert a timestamp into a human-readable date string.

.. rubric:: Properties

**script:** Javascript defining how to transform one record into another. The script must
implement a function called 'transform', which takes as input a JSON object that represents
the input record, and returns a JSON object that represents the transformed record.

**schema:** The schema of output objects. If no schema is given, it is assumed that the output
schema is the same as the input schema.

.. rubric:: Example

::

  {
    "name": "Script",
    "properties": {
      "script": "function transform(input) {
                   var tax = input.subtotal * 0.0975;
                   return {
                     'subtotal': input.subtotal,
                     'tax': tax,
                     'total': input.subtotal + tax
                   };
                 }",
      "schema": "{
        \"type\":\"record\",
        \"name\":\"expanded\",
        \"fields\":[
          {\"name\":\"subtotal\",\"type\":\"double\"},
          {\"name\":\"tax\",\"type\":\"double\"},
          {\"name\":\"total\",\"type\":\"double\"}
        ]
      }"
    }
  }

The transform takes records that have a 'subtotal' field, calculates 'tax' and 'total' fields based on the
subtotal, and then returns a record containing those three fields.
For example, if it receives as an input record::

  +=========================================================+
  | field name | type                | value                |
  +=========================================================+
  | subtotal   | double              | 100.0                |
  | user       | string              | "samuel"             |
  +=========================================================+

It will transform it to an output record::

  +=========================================================+
  | field name | type                | value                |
  +=========================================================+
  | subtotal   | double              | 100.0                |
  | tax        | double              | 9.75                 |
  | total      | double              | 109.75               |
  +=========================================================+
