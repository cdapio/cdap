# Data Generator


Description
-----------
Source that can generate test data for real-time Stream and Table sinks.


Use Case
--------
The source is used purely for testing purposes. For example, suppose you have added a
custom transform that you wish to test. Instead of hooking up the transform to a real
source, you decide to use the test source to make things easier on yourself and to
prevent adding load to other systems.


Properties
----------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**type:** The type of data to be generated. Currently, only two types -- 'stream' and
'table' -- are supported. By default, it generates a structured record containing one
field named 'data' of type String with the value 'Hello'.

If the type is set to 'stream', it will output records with this schema:

    +===================================+
    | field name  | type                |
    +===================================+
    | body        | string              |
    | headers     | map<string, string> |
    +===================================+

If the type is set to 'table', it will output records with this schema:

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
    

Example
-------

    {
        "name": "DataGenerator",
        "type": "realtimesource",
        "properties": {
            "type": "table"
        }
    }

---
- CDAP Pipelines Plugin Type: realtimesource
- CDAP Pipelines Version: 1.7.0
