.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _included-apps-etl-plugins-transformations-structuredrecordtogenericrecord:

=================================================
Transformations: StructuredRecordToGenericRecord 
=================================================

.. rubric:: Description

Transforms a StructuredRecord into an Avro GenericRecord. 

``StructuredRecord`` is the Java class that all built-in plugins work with. Most
``StructuredRecord``\s can be directly converted to a ``GenericRecord``. An exception is if the
``StructuredRecord`` contains a map field with keys that are not of type ``'string'``.

.. rubric:: Use Case

The transform is used whenever you need to use an Avro ``GenericRecord``. For example, if
you have a custom sink that accepts as input ``GenericRecord``\s, you will use this
transform right before the sink.

.. rubric:: Properties

The transform does not take any properties.

.. rubric:: Example

::

  {
    "name": "StructuredRecordToGenericRecord",
    "properties": { }
  }
