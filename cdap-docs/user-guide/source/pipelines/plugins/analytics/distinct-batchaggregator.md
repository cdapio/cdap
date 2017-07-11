# Distinct Aggregator

Description
-----------
De-duplicates input records so that all output records are distinct.
Can optionally take a list of fields, which will project out all other fields and perform a distinct on just those fields.


Use Case
--------
This plugin is used when you want to ensure that all output records are unique.


Properties
----------
**fields:** Optional comma-separated list of fields to perform the distinct on. If not given, all fields are used.

**numPartitions:** Number of partitions to use when grouping fields. If not specified, the execution
framework will decide on the number to use.

Example
-------

    {
        "name": "Distinct",
        "type": "batchaggregator"
        "properties": {
            "fields": "user,item,action"
        }
    }


This example takes the ``user``, ``action``, and ``item`` fields from input records and dedupes them so that every
output record is a unique record with those three fields. For example, if the input to the plugin is:

     +=====================================+
     | user  | item   | action | timestamp |
     +=====================================+
     | bob   | donut  | buy    | 1000      |
     | bob   | donut  | buy    | 1000      |
     | bob   | donut  | buy    | 1001      |
     | bob   | coffee | buy    | 1001      |
     | bob   | coffee | drink  | 1010      |
     | bob   | donut  | eat    | 1050      |
     | bob   | donut  | eat    | 1080      |
     +=====================================+

then records output will be:

     +=========================+
     | user  | item   | action |
     +=========================+
     | bob   | donut  | buy    |
     | bob   | coffee | buy    |
     | bob   | coffee | drink  |
     | bob   | donut  | eat    |
     +=========================+

---
- CDAP Pipelines Plugin Type: batchaggregator
- CDAP Pipelines Version: 1.7.0
