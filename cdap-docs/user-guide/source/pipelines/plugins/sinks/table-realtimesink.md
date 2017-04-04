# Table


Description
-----------
Writes records to a CDAP Table with one record field mapping
to the Table rowkey, and all other record fields mapping to Table columns.


Use Case
--------
The sink is used whenever you need to write to a Table in real-time. For example,
you may want to read from Kafka and write to a Table.


Properties
----------
**name:** Name of the table dataset. If it does not already exist, one will be created.

**schema:** Optional schema of the table as a JSON Object. If the table does not
already exist, one will be created with this schema, which will allow the table to be
explored through Hive.

**schema.row.field:** The name of the record field that should be used as the row
key when writing to the table.

**case.sensitive.row.field:** Whether 'schema.row.field' is case sensitive; defaults to true.


Example
-------
The example writes to a Table named 'users':

    {
        "name": "Table",
        "type": "realtimesink",
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

It takes records with this schema as input:

    +======================================+
    | field name     | type                |
    +======================================+
    | id             | long                |
    | name           | string              |
    | birthyear      | int                 |
    +======================================+

The 'id' field will be used as the rowkey when writing to the table. The 'name' and 'birthyear' record
fields will be written to columns named 'name' and 'birthyear'.

---
- CDAP Pipelines Plugin Type: realtimesink
- CDAP Pipelines Version: 1.7.0
