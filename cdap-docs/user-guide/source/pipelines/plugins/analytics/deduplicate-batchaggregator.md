# Deduplicate


Description
-----------
De-duplicates records either using one or more fields or by using the record as a whole. Additionally, it supports logically
choosing a record out of the duplicate records based on a filter field. Supported logical functions are `max`, `min`,
`first`, and `last`.

Use Case
--------
The aggregator is used when you want to filter out duplicates in the input in a predictable way.

Properties
----------
**uniqueFields:** An optional comma-separated list of fields on which to perform the deduplication. If none given, each 
record will be considered as a whole for deduplication. For example, if the input contain records with fields `fname`, 
`lname`, `item`, and `cost`, and we want to deduplicate the records by name, then this property should be set to 
`fname,lname`.

**filterOperation:** An optional property that can be set to predictably choose one or more records from the set of records
that needs to be deduplicated. This property takes in a field name and the logical operation that needs to be performed
on that field on the set of records. The syntax is `field:function`. For example, if we want to choose the record with
maximum cost for the records with schema `fname`, `lname`, `item`, `cost`, then this field should be set as `cost:max`.
Supported functions are `first`, `last`, `max`, and `min`. Note that only one pair of field and function is allowed.
If this property is not set, one random record will be chosen from the group of 'duplicate' records.

**numPartitions:** An optional number of partitions to use when grouping unique fields. If not specified, the execution
framework will decide on the number to use.

Example
-------
This example deduplicates records by their `fname` and `lname` fields. Then, it chooses one record out of the
duplicates based on the `cost` field. Since the function specified is `max`, the record with the maximum value in the
`cost` field is chosen out of each set of duplicate records.

    {
        "name": "Deduplicate",
        "type": "batchaggregator",
        "properties": {
            "uniqueFields": "fname,lname",
            "filterOperation": "cost:max"
        }
    }


For example, suppose the aggregator receives input records where each record represents a purchase:

    +======================================+
    | fname  | lname   | cost   |  zipcode |
    +======================================+
    | bob    | smith   | 50.23  |  12345   |
    | bob    | jones   | 30.64  |  23456   |
    | alice  | smith   | 1.50   |  34567   |
    | bob    | smith   | 0.50   |  45678   |
    | alice  | smith   | 30.21  |  56789   |
    | alice  | jones   | 500.93 |  67890   |
    +======================================+

Output records will contain one record for each `fname,lname` combination that has the maximum `cost`:

    +======================================+
    | fname  | lname   | cost   |  zipcode |
    +======================================+
    | bob    | smith   | 50.23  |  12345   |
    | bob    | jones   | 30.64  |  23456   |
    | alice  | smith   | 30.21  |  56789   |
    | alice  | jones   | 500.93 |  67890   |
    +======================================+

---
- CDAP Pipelines Plugin Type: batchaggregator
- CDAP Pipelines Version: 1.7.0
