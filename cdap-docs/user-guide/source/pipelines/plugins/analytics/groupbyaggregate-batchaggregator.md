# GroupBy Aggregate


Description
-----------
Groups by one or more fields, then performs one or more aggregate functions on each group.
Supports `avg`, `count`, `count(*)`, `first`, `last`, `max`, `min`,`sum` as aggregate functions.

Use Case
--------
The transform is used when you want to calculate some basic aggregations in your data similar
to what you could do with a group-by query in SQL.

Properties
----------
**groupByFields:** Comma-separated list of fields to group by.
Records with the same value for all these fields will be grouped together.
Records output by this aggregator will contain all the group by fields and aggregate fields.
For example, if grouping by the ``user`` field and calculating an aggregate ``numActions:count(*)``,
output records will have a ``user`` field and a ``numActions`` field.

**aggregates:** Aggregates to compute on each group of records.
Supported aggregate functions are `avg`, `count`, `count(*)`, `first`, `last`, `max`, `min`,`sum`.
A function must specify the field it should be applied on, as well as the name it should be called.
Aggregates are specified using the syntax `name:function(field)[, other aggregates]`.
For example, ``avgPrice:avg(price),cheapest:min(price)`` will calculate two aggregates.
The first will create a field called ``avgPrice`` that is the average of all ``price`` fields in the group.
The second will create a field called ``cheapest`` that contains the minimum ``price`` field in the group.
The count function differs from count(*) in that it contains non-null values of a specific field,
while count(*) will count all records regardless of value.

**numPartitions:** Number of partitions to use when grouping fields. If not specified, the execution
framework will decide on the number to use.

Example
-------
This example groups records by their ``user`` and ``item`` fields.
It then calculates two aggregates for each group. The first is a sum on ``price``
and the second counts the number of records in the group.

    {
        "name": "GroupByAggregate",
        "type": "batchaggregator",
        "properties": {
            "groupByFields": "user,item",
            "aggregates": "totalSpent:sum(price),numPurchased:count(*)"
        }
    }


For example, suppose the aggregator receives input records where each record represents a purchase:

    +========================+
    | user  | item   | price |
    +========================+
    | bob   | donut  | 0.80  |
    | bob   | coffee | 2.05  |
    | bob   | donut  | 1.50  |
    | bob   | donut  | 0.50  |
    | alice | tea    | 1.99  |
    | alice | cookie | 0.50  |
    +========================+

Output records will contain all group fields in addition to a field for each aggregate:

    +============================================+
    | user  | item   | totalSpent | numPurchased |
    +============================================+
    | bob   | donut  | 2.80       | 3            |
    | bob   | coffee | 2.05       | 1            |
    | alice | tea    | 1.99       | 1            |
    | alice | cookie | 0.50       | 1            |
    +============================================+

---
- CDAP Pipelines Plugin Type: batchaggregator
- CDAP Pipelines Version: 1.7.0
