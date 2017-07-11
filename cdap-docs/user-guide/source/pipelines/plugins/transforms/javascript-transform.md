# JavaScript


Description
-----------
Executes user-provided JavaScript that transforms one record into zero or more records.
Input records are converted into JSON objects which can be directly accessed in
JavaScript. The transform expects to receive a JSON object as input, which it can
process and emit zero or more records or emit error using the provided emitter object.


Use Case
--------
The JavaScript transform is used when other transforms cannot meet your needs.
For example, you may want to multiply a field by 1024 and rename it from ``'gigabytes'``
to ``'megabytes'``. Or you might want to convert a timestamp into a human-readable date string.


Properties
----------
**script:** JavaScript defining how to transform input record into zero or more records. The script must
implement a function called ``'transform'``, which takes as input a JSON object (representing
the input record), an emitter object (to emit zero or more output records), 
and a context object (which encapsulates CDAP metrics, logger, and lookup).
For example:

    function transform(input, emitter, context) {
        if (context.getLookup('blacklist').lookup(input.id) != null) {
            emitter.emitError({
                'errorCode': 31,
                'errorMsg': 'blacklisted id',
                'invalidRecord': input
            });
        } else {
            if (input.count < 0) {
                context.getMetrics().count('negative.count', 1);
                context.getLogger().debug('Received record with negative count');
            }
            input.count = input.count * 1024;
            emitter.emit(input);
        }
    }

will emit an error if the ``id`` field is present in blacklist table, else scale the ``count`` field by 1024.

**schema:** The schema of output objects. If no schema is given, it is assumed that the output
schema is the same as the input schema.

**lookup:** The configuration of the lookup tables to be used in your script.
For example, if lookup table "purchases" is configured, then you will be able to perform
operations with that lookup table in your script: ``context.getLookup('purchases').lookup('key')``
Currently supports ``KeyValueTable``.


Example
-------

    {
        "name": "JavaScript",
        "type": "transform",
        "properties": {
            "script": "function transform(input, emitter, context) {
                   var tax = input.subtotal * 0.0975;
                   if (tax > 1000.0) {
                     context.getMetrics().count("tax.above.1000", 1);
                   }
                   if (tax < 0.0) {
                     context.getLogger().info("Received record with negative subtotal");
                   }
                   emitter.emit( {
                     'subtotal': input.subtotal,
                     'tax': tax,
                     'total': input.subtotal + tax
                   });
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
fields. For example, if it receives as an input record:

    +=========================================================+
    | field name | type                | value                |
    +=========================================================+
    | subtotal   | double              | 100.0                |
    | user       | string              | "samuel"             |
    +=========================================================+

it will transform it to this output record:

    +=========================================================+
    | field name | type                | value                |
    +=========================================================+
    | subtotal   | double              | 100.0                |
    | tax        | double              | 9.75                 |
    | total      | double              | 109.75               |
    +=========================================================+

---
- CDAP Pipelines Plugin Type: transform
- CDAP Pipelines Version: 1.7.0
