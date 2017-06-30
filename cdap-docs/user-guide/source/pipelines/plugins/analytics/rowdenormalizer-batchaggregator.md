# Row Denormalizer


Description
-----------
Converts raw data into denormalized data based on a key column. User is able to specify the list of fields that should be used in the denormalized record, with an option to use an alias for the output field name. For example, 'ADDRESS' in the input is mapped to 'addr' in the output schema. 

Use Case
--------
The transform takes input record that stores a variable set of custom attributes for an entity, denormalizes it on the basis of the key field, and then returns a denormalized table according to the output schema specified by the user.
The denormalized data is easier to query.

Properties
----------
**keyField:** Name of the column in the input record which will be used to group the raw data. For Example, id.

**nameField:** Name of the column in the input record which contains the names of output schema columns. For example,
 input records have columns 'id', 'attribute', 'value' and the 'attribute' column contains 'FirstName', 'LastName',
 'Address'.
  "So the output record will have column names as 'FirstName', 'LastName', 'Address'.

**valueField:** Name of the column in the input record which contains the values for output schema columns. For
example, input records have columns 'id', 'attribute', 'value' and the 'value' column contains 'John',
'Wagh', 'NE Lakeside'. So the output record will have values for columns 'FirstName', 'LastName', 'Address' as 'John', 'Wagh', 'NE Lakeside' respectively.

**outputFields:** List of the output fields to be included in denormalized output.

**fieldAliases:** List of the output fields to rename. The key specifies the name of the field to rename, with its corresponding value specifying the new name for that field.

**numPartitions:** Number of partitions to use when grouping data. If not specified, the execution framework will
decide on the number to use.

Conditions
----------
In case a field value is not present, then it will be considered as NULL.

For Example,

If keyfield('id') in the input record is NULL, then that particular record will be filtered out.

If namefield('attribute') or valuefield('value') is not present for a particular keyfield('id') value, then the
denormalized output value for that namefield will be NULL.

If user provides output field which is not present in the input record, then it will be considered as NULL.

Example
-------
The transform takes input records that have columns id, attribute, value, denormalizes it on the basis of
id, and then returns a denormalized table according to the output schema specified by the user.

    {
      "name": "RowDenormalizer",
      "type": "batchaggregator",
      "properties": {
         "outputFields": "Firstname,Lastname,Address",
         "fieldAliases": "Address:Office Address",
         "keyField": "id",
         "nameField": "attribute",
         "valueField": "value"
       }
    }

For example, suppose the aggregator receives the input record:

    +======================================+
    | id        | attribute   | value      |
    +======================================+
    | joltie    | Firstname   | John       |
    | joltie    | Lastname    | Wagh       |
    | joltie    | Address     | NE Lakeside|
    +======================================+

Output records will contain all the output fields specified by user:

    +=========================================================+
    | id        | Firstname   | Lastname   |  Office Address  |
    +=========================================================+
    | joltie    | John        | Wagh       |  NE Lakeside     |
    +=========================================================+


Now, let's suppose the aggregator receives the input record with NULL values:

    +======================================+
    | id        | attribute   | value      |
    +======================================+
    | joltie    | Firstname   | John       |
    | joltie    | Lastname    | Wagh       |
    | joltie    | Address     | NE Lakeside|
    | brett     | Firstname   | Brett      |
    |           | Lastname    | Lee        |
    | brett     | Address     | SE Lakeside|
    | bob       | Firstname   | Bob        |
    | bob       |             | Smith      |
    | bob       | Address     |            |
    +======================================+

Output records will contain all the output fields specified by user:

    +=========================================================+
    | id        | Firstname   | Lastname   |  Office Address  |
    +=========================================================+
    | joltie    | John        | Wagh       |  NE Lakeside     |
    | brett     | Brett       |            |  SE Lakeside     |
    | bob       | Bob         |            |                  |
    +=========================================================+

---
- CDAP Pipelines Plugin Type: batchaggregator
- CDAP Pipelines Version: 1.7.0
