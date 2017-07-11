# StructuredRecord To GenericRecord


Description
-----------
Transforms a StructuredRecord into an Avro GenericRecord. 

``StructuredRecord`` is the Java class that all built-in plugins work with. Most
``StructuredRecord``s can be directly converted to a ``GenericRecord``. An exception is if the
``StructuredRecord`` contains a map field with keys that are not of type ``'string'``.


Use Case
--------
The transform is used whenever you need to use an Avro ``GenericRecord``. For example, if
you have a custom sink that accepts as input ``GenericRecord``s, you will use this
transform right before the sink.


Properties
----------
The transform does not take any properties.


Example
-------

    {
        "name": "StructuredRecordToGenericRecord",
        "type": "transform",
        "properties": { }
    }

---
- CDAP Pipelines Plugin Type: transform
- CDAP Pipelines Version: 1.7.0
