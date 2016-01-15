.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _cdap-apps-etl-plugins-transformations-script:

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
For example, you may want to multiply a field by 1024 and rename it from ``'gigabytes'``
to ``'megabytes'``. Or you might want to convert a timestamp into a human-readable date string.

.. rubric:: Properties

**script:** Javascript defining how to transform one record into another. The script must
implement a function called ``'transform'``, which takes as input a JSON object (representing
the input record) and a context object (which encapsulates CDAP metrics and logger),
and returns a JSON object that represents the transformed input.
For example::

  function transform(input, context) {
    if(input.count < 0) {
      context.getMetrics().count("negative.count", 1);
      context.getLogger().debug("Received record with negative count");
    }
    input.count = input.count * 1024;
    return input;
  }

will scale the ``'count'`` field by 1024.

**schema:** The schema of output objects. If no schema is given, it is assumed that the output
schema is the same as the input schema.

**lookup:** The configuration of the lookup tables to be used in your script.
For example, if lookup table "purchases" is configured, then you will be able to perform
operations with that lookup table in your script: ``context.getLookup('purchases').lookup('key')``
Currently supports ``KeyValueTable``.

.. rubric:: Example

::

  {
    "name": "Script",
    "properties": {
      "script": "function transform(input, context) {
                   var tax = input.subtotal * 0.0975;
                   if (tax > 1000.0) {
                     context.getMetrics().count("tax.above.1000", 1);
                   }
                   if (tax < 0.0) {
                     context.getLogger().info("Received record with negative subtotal");
                   }
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
      }",
      "lookup": "{
        \"tables\":{
          \"purchases\":{
            \"type\":\"DATASET\",
            \"datasetProperties\":{
              \"dataset_argument1\":\"foo\",
              \"dataset_argument2\":\"bar\"
            }
          }
        }
      }"
    }
  }

The transform takes records that have a ``'subtotal'`` field, calculates ``'tax'`` and
``'total'`` fields based on the subtotal, and then returns a record containing those three
fields. For example, if it receives as an input record::

  +=========================================================+
  | field name | type                | value                |
  +=========================================================+
  | subtotal   | double              | 100.0                |
  | user       | string              | "samuel"             |
  +=========================================================+

it will transform it to this output record::

  +=========================================================+
  | field name | type                | value                |
  +=========================================================+
  | subtotal   | double              | 100.0                |
  | tax        | double              | 9.75                 |
  | total      | double              | 109.75               |
  +=========================================================+


**Note:** These default metrics are emitted by this transform:

.. csv-table::
   :header: "Metric Name","Description"
   :widths: 40,60

   "``records.in``","Input records processed by this transform stage"
   "``records.out``","Output records sent to the next stage"

