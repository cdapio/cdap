# Database


Description
-----------
Action that runs a database command.


Use Case
--------
The action can be used whenever you want to run a database command before or after a data pipeline.
For example, you may want to run a sql update command on a database before the pipeline source pulls data from tables.


Properties
----------
**query:** The database command to execute.

**connectionString:** JDBC connection string including database name.

**user:** User identity for connecting to the specified database. Required for databases that need
authentication. Optional for databases that do not require authentication.

**password:** Password to use to connect to the specified database. Required for databases
that need authentication. Optional for databases that do not require authentication.

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
It will run an update command to set the price of record with ID 6 to 20.

    {
        "name": "Database",
        "plugin": {
            "name": "Database",
            "type": "action",
            "properties": {
                "query": "UPDATE table_name SET price = 20 WHERE ID = 6",
                "connectionString": "jdbc:postgresql://localhost:5432/prod",
                "user": "user123",
                "password": "password-abc",
                "jdbcPluginName": "postgres",
                "jdbcPluginType": "jdbc"
            }
        }
    }

---
- CDAP Pipelines Plugin Type: action
- CDAP Pipelines Version: 1.7.0
