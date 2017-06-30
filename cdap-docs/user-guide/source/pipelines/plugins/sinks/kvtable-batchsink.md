# KeyValueTable


Description
-----------
Writes records to a KeyValueTable, using configurable fields from input records as the
key and value.

Use Case
--------
The source is used whenever you need to write to a KeyValueTable in batch. For example,
you may want to periodically copy portions of a Table into a KeyValueTable.

Properties
----------
**name:** Name of the dataset. If it does not already exist, one will be created.

**key.field:** The name of the field to use as the key. Defaults to 'key'.

**value.field:** The name of the field to use as the value. Defaults to 'value'.

Example
-------
This example writes to a KeyValueTable named 'items':

    {
        "name": "KVTable",
        "type": "batchsink",
        "properties": {
            "name": "items",
            "key.field": "id",
            "value.field": "description"
        }
    }

It takes records with the following schema as input:

    +======================================+
    | field name     | type                |
    +======================================+
    | id             | bytes               |
    | description    | bytes               |
    +======================================+

When writing to the KeyValueTable, the 'id' field will be used as the key,
and the 'description' field will be used as the value.

---
- CDAP Pipelines Plugin Type: batchsink
- CDAP Pipelines Version: 1.7.0
