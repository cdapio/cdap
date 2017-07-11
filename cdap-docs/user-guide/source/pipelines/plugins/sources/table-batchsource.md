# Table


Description
-----------
Reads the entire contents of a CDAP Table. Outputs one record for each row in the Table.
The Table must conform to a given schema. 


Use Case
--------
The source is used whenever you need to read from a table in batch. For example,
you may want to periodically dump the contents of a CDAP Table to a relational database.


Properties
----------
**name:** Table name. If the table does not already exist, it will be created. (Macro-enabled)

**schema:** Schema of records read from the table. Row columns map to record
fields. For example, if the schema contains a field named 'user' of type string, the value
of that field will be taken from the value stored in the 'user' column. Only simple types
are allowed (boolean, int, long, float, double, bytes, string). (Macro-enabled)

**schema.row.field:** Optional record field for which row key will be considered as value instead of row column.
The field name specified must be present in the schema, and must not be nullable. (Macro-enabled)


Example
-------
This example reads from a Table named 'users':

    {
        "name": "Table",
        "type": "batchsource",
        "properties": {
            "name": "users",
            "schema": "{
                \"type\":\"record\",
                \"name\":\"user\",
                \"fields\":[
                    {\"name\":\"id\",\"type\":\"long\"},
                    {\"name\":\"name\",\"type\":\"string\"},
                    {\"name\":\"birthyear\",\"type\":\"int\"}
                ]
            }",
            "schema.row.field": "id"
        }
    }

It outputs records with this schema:

    +======================================+
    | field name     | type                |
    +======================================+
    | id             | long                |
    | name           | string              |
    | birthyear      | int                 |
    +======================================+

The 'id' field will be read from the row key of the table. The 'name' field will be read from the
'name' column in the table. The 'birthyear' field will be read from the 'birthyear' column in the
table. Any other columns in the Table will be ignored by the source.

---
- CDAP Pipelines Plugin Type: batchsource
- CDAP Pipelines Version: 1.7.0
