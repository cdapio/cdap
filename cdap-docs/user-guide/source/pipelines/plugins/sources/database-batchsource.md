# Database


Description
-----------
Reads from a database using a configurable SQL query.
Outputs one record for each row returned by the query.


Use Case
--------
The source is used whenever you need to read from a database. For example, you may want
to create daily snapshots of a database table by using this source and writing to
a TimePartitionedFileSet.


Properties
----------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**importQuery:** The SELECT query to use to import data from the specified table.
You can specify an arbitrary number of columns to import, or import all columns using \*. The Query should
contain the '$CONDITIONS' string. For example, 'SELECT * FROM table WHERE $CONDITIONS'.
The '$CONDITIONS' string will be replaced by 'splitBy' field limits specified by the bounding query.
The '$CONDITIONS' string is not required if numSplits is set to one. (Macro-enabled)

**boundingQuery:** Bounding Query should return the min and max of the values of the 'splitBy' field.
For example, 'SELECT MIN(id),MAX(id) FROM table'. Not required if numSplits is set to one. (Macro-enabled)

**splitBy:** Field Name which will be used to generate splits. Not required if numSplits is set to one. (Macro-enabled)

**numSplits:** Number of splits to generate. (Macro-enabled)

**columnCase:** Sets the case of the column names returned from the query.
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

**enableAutoCommit:** Whether to enable auto-commit for queries run by this source. Defaults to 'false'.
Normally this setting does not matter. It only matters if you are using a jdbc driver -- like the Hive
driver -- that will error when the commit operation is run, or a driver that will error when auto-commit is
set to false. For drivers like those, you will need to set this to 'true'.


Example
-------
This example connects to a database using the specified 'connectionString', which means
it will connect to the 'prod' database of a PostgreSQL instance running on 'localhost'.
It will run the 'importQuery' against the 'users' table to read four columns from the table.
The column types will be used to derive the record field types output by the source.

    {
        "name": "Database",
        "type": "batchsource",
        "properties": {
            "importQuery": "select id,name,email,phone from users where $CONDITIONS",
            "boundingQuery": "select min(id),max(id) from users",
            "splitBy": "id",
            "connectionString": "jdbc:postgresql://localhost:5432/prod",
            "user": "user123",
            "password": "password-abc",
            "jdbcPluginName": "postgres",
            "jdbcPluginType": "jdbc"
        }
    }

For example, if the 'id' column is a primary key of type int and the other columns are
non-nullable varchars, output records will have this schema:

    +======================================+
    | field name     | type                |
    +======================================+
    | id             | int                 |
    | name           | string              |
    | email          | string              |
    | phone          | string              |
    +======================================+

Using Third-Party JARs
----------------------
For information on how to use the JDBC jar to talk to the database source, see
[Using Third-Party JARs](../../third-party.html).

---
- CDAP Pipelines Plugin Type: batchsource
- CDAP Pipelines Version: 1.7.0
