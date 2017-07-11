# Database


Description
-----------
Writes records to a database table. Each record will be written to a row in the table.


Use Case
--------
This sink is used whenever you need to write to a database table.
Suppose you periodically build a recommendation model for products on your online store.
The model is stored in a FileSet and you want to export the contents
of the FileSet to a database table where it can be served to your users.


Properties
----------
**referenceName:** This will be used to uniquely identify this sink for lineage, annotating metadata, etc.

**tableName:** Name of the table to export to. (Macro-enabled)

**columns:** Comma-separated list of columns in the specified table to export to.

**columnCase:** Sets the case of the column names returned by the column check query.
Possible options are ``upper`` or ``lower``. By default or for any other input, the column names are not modified and
the names returned from the database are used as-is. Note that setting this property provides predictability
of column name cases across different databases but might result in column name conflicts if multiple column
names are the same when the case is ignored (optional).

**connectionString:** JDBC connection string including database name. (Macro-enabled)

**user:** User identity for connecting to the specified database. Required for databases that need
authentication. Optional for databases that do not require authentication. (Macro-enabled)

**password:** Password to use to connect to the specified database. Required for databases
that need authentication. Optional for databases that do not require authentication. (Macro-enabled)

**jdbcPluginName:** Name of the JDBC plugin to use. This is the value of the 'name' key
defined in the JSON file for the JDBC plugin.

**jdbcPluginType:** Type of the JDBC plugin to use. This is the value of the 'type' key
defined in the JSON file for the JDBC plugin. Defaults to 'jdbc'.

**enableAutoCommit:** Whether to enable auto-commit for queries run by this sink. Defaults to 'false'.
Normally this setting does not matter. It only matters if you are using a jdbc driver -- like the Hive
driver -- that will error when the commit operation is run, or a driver that will error when auto-commit is
set to false. For drivers like those, you will need to set this to 'true'.

**schema:** The schema of records output by the source. This will be used in place of whatever schema comes 
back from the query. However, it must match the schema that comes back from the query, 
except it can mark fields as nullable and can contain a subset of the fields. 

Example
-------
This example connects to a database using the specified 'connectionString', which means
it will connect to the 'prod' database of a PostgreSQL instance running on 'localhost'.
Each input record will be written to a row of the 'users' table, with the value for each
column taken from the value of the field in the record. For example, the 'id' field in
the record will be written to the 'id' column of that row.

    {
        "name": "Database",
        "type": "batchsink",
        "properties": {
            "tableName": "users",
            "columns": "id,name,email,phone",
            "connectionString": "jdbc:postgresql://localhost:5432/prod",
            "user": "postgres",
            "password": "",
            "jdbcPluginName": "postgres",
            "jdbcPluginType": "jdbc"
        }
    }

Using Third-Party JARs
----------------------
For information on how to use the JDBC jar to talk to the database sink, see
[Using Third-Party JARs](../../third-party.html).

---
- CDAP Pipelines Plugin Type: batchsink
- CDAP Pipelines Version: 1.7.0
