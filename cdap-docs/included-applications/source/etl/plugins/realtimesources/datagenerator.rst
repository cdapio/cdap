.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _included-apps-etl-plugins-real-time-sources-datagenerator:

=================================
Real-time Sources: DataGenerator 
=================================

.. rubric:: Description

Source that can generate test data for real-time Stream and Table sinks.

.. rubric:: Use Case

The source is used purely for testing purposes. For example, suppose you have added a
custom transform that you wish to test. Instead of hooking up the transform to a real
source, you decide to use the test source to make things easier on yourself and to
prevent adding load to other systems.

.. rubric:: Properties

**type:** The type of data to be generated. Currently, only two types |---| 'stream' and
'table' |---| are supported. By default, it generates a structured record containing one
field named 'data' of type String with the value 'Hello'.

If the type is set to 'stream', it will output records with this schema::

  +===================================+
  | field name  | type                |
  +===================================+
  | body        | string              |
  | headers     | map<string, string> |
  +===================================+

If the type is set to 'table', it will output records with this schema::

  +===================================+
  | field name  | type                |
  +===================================+
  | id          | int                 |
  | name        | string              |
  | score       | double              |
  | graduated   | boolean             |
  | binary      | bytes               |
  | time        | long                |
  +===================================+

.. rubric:: Example

::

  {
    "name": "DataGenerator",
    "properties": {
      "type": "table"
    }
  }
