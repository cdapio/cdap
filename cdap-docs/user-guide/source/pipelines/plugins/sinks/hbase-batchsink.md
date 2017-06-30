# HBase


Description
-----------
Writes records to a column family in an HBase table with one record field mapping
to the rowkey, and all other record fields mapping to table column qualifiers.
This sink differs from the Table sink in that it does not use CDAP datasets, but writes
to HBase directly.


Use Case
--------
The sink is used whenever you need to write to an HBase table in batch. For example,
you may want to periodically dump the contents of a relational database into an HBase table.


Properties
----------
**referenceName:** This will be used to uniquely identify this sink for lineage, annotating metadata, etc.

**tableName:** The name of the table to write to. **Note:** Prior to running the pipeline,
this table should already exist. (Macro-enabled)

**columnFamily:** The name of the column family to write to. (Macro-enabled)

**schema:** Schema of records written to the table. Record fields map to row columns. For
example, if the schema contains a field named 'user' of type string, the value of that
field will be written to the 'user' column. Only simple types are allowed (boolean, int,
long, float, double, bytes, string).

**rowField:** Field name indicating that the field value should
be written as the rowkey instead of written to a column. The field name specified must be present in
the schema, and must not be nullable.

**zkQuorum:** The ZooKeeper quorum for the hbase instance you are writing to. This should
be a comma-separated list of hosts that make up the quorum. You can find the correct value
by looking at the ``hbase.zookeeper.quorum`` setting in your ``hbase-site.xml`` file. This value
defaults to ``'localhost'``. (Macro-enabled)

**zkClientPort:** The client port used to connect to the ZooKeeper quorum.
You can find the correct value by looking at the ``hbase.zookeeper.quorum`` setting in your ``hbase-site.xml``.
This value defaults to ``2181``. (Macro-enabled)

**zkNodeParent:** The parent node of HBase in ZooKeeper. 
You can find the correct value by looking at the ``hbase.zookeeper.quorum`` setting in your ``hbase-site.xml``.
This value defaults to ``'/hbase'``.


Example
-------
This example writes to the 'attr' column family of an HBase table named 'users':

    {
        "name": "Table",
        "type": "batchsink",
        "properties": {
            "tableName": "users",
            "columnFamily": "attr",
            "rowField": "id",
            "zkQuorum": "host1,host2,host3",
            "zkClientPort": "2181",
            "zkNodeParent": "/hbase",
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
fields will be written to column qualifiers named 'name' and 'birthyear'.

---
- CDAP Pipelines Plugin Type: batchsink
- CDAP Pipelines Version: 1.7.0
