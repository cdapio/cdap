.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

==================
Connection Service
==================

The Connection Service provides RESTful APIs for managing the lifecycle
of connections. All connection information is stored in the connections
store of the dataset.

These are the lifecycle operations supported by the connection service:

-  Create a new connection (**POST**, ``${base}/connections/create``)
-  Update an entire connection (**POST**,
   ``${base}/connections/{id}/update``)
-  Update properties of a connection (**PUT**,
   ``${base}/connections/{id}/properties?key=<key>&value=<value>``)
-  Retrieve all properties of a connection (**GET**,
   ``${base}/connections/{id}/properties``)
-  Delete a connection (**DELETE**, ``${base}/connections/{id}``)
-  Clone a connection (**GET**, ``${base}/connections/{id}/clone``)
-  Retrieve information about all of the connections (**GET**,
   ``${base}/connections``)
-  Retrieve information about a connection (**GET**
   ``${base}/connections/{id}``)

Base
----

This is the base URL for the service:

::

    http://localhost:11015/v3/namespaces/<namespace>/apps/dataprep/services/service/methods

    NOTE: All examples below use a 'default' namespace.

Request JSON Object for creation and complete update
----------------------------------------------------

These are the fields that can be in the request:

-  name (mandatory)
-  description (optional)
-  type (mandatory; one of:)
-  DATABASE
-  KAFKA
-  S3
-  properties (optional)

Here is an example of a JSON Request for creating a connection:

::

    {
        "name": "MySQL Database",
        "description": "MySQL Configuration",
        "type": "DATABASE",
        "properties": {
            "hostaname": "localhost",
            "port": "3306"
        }
    }

Upon successful creation, the ID of the entry is returned. Here is an
example response when creation is successful:

::

    {
        "status": 200,
        "message": "Success",
        "count": 1,
        "values": [
            "mysql_database"
        ]
    }

Sample Runs
-----------

Connection JSON
~~~~~~~~~~~~~~~

::

    cat /Users/nitin/Work/Demo/data/mysql.connection.json
    {
      "name":"MySQL Database",
      "type":"DATABASE",
      "description":"MySQL Configuration",
      "properties" : {
        "hostaname" : "localhost",
        "port" : 3306
      }
    }

Create REST API call.
~~~~~~~~~~~~~~~~~~~~~

::

    curl -s --data "@/tmp/mysql.connection.json" 'http://localhost:11015/v3/namespaces/default/apps/dataprep/services/service/methods/connections/create' | python -mjson.tool
    {
        "count": 1,
        "message": "Success",
        "status": 200,
        "values": [
            "mysql_database"
        ]
    }  

Repeat creation will fail
~~~~~~~~~~~~~~~~~~~~~~~~~

::

    curl -s --data "@/Users/nitin/Work/Demo/data/mysql.connection.json" 'http://localhost:11015/v3/namespaces/default/apps/dataprep/services/service/methods/connections/create' | python -mjson.tool
    {
        "message": "Connection name 'MySQL Database' already exists.",
        "status": 500
    }

Delete Connection
~~~~~~~~~~~~~~~~~

::

    curl -X DELETE "http://localhost:11015/v3/namespaces/default/apps/dataprep/services/service/methods/connections/mysql_database" | python -mjson.tool
    {
         "status":200,
         "message":"Success"
    }

Repeated delete will also be successful or even when the key is not found.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    curl -s -X DELETE "http://localhost:11015/v3/namespaces/default/apps/dataprep/services/service/methods/connections/mysql_database" | python -mjson.tool
    {
        "message": "Success",
        "status": 200
    }

Listing All Connections
~~~~~~~~~~~~~~~~~~~~~~~

::

    curl -s "http://localhost:11015/v3/namespaces/default/apps/dataprep/services/service/methods/connections?type=*" | python -mjson.tool
    {
        "count": 1,
        "message": "Success",
        "status": 200,
        "values": [
            {
                "created": 1494529821,
                "description": "MySQL Configuration",
                "id": "mysql_database",
                "name": "MySQL Database",
                "type": "DATABASE",
                "updated": 1494529821
            }
        ]
    }

Listing Only connections of type Database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    curl -s "http://localhost:11015/v3/namespaces/default/apps/dataprep/services/service/methods/connections?type=database" | python -mjson.tool
    {
        "count": 1,
        "message": "Success",
        "status": 200,
        "values": [
            {
                "created": 1494529821,
                "description": "MySQL Configuration",
                "id": "mysql_database",
                "name": "MySQL Database",
                "type": "DATABASE",
                "updated": 1494529821
            }
        ]
    }

Info about connection
~~~~~~~~~~~~~~~~~~~~~

::

    curl -s "http://localhost:11015/v3/namespaces/default/apps/dataprep/services/service/methods/connections/mysql_database" | python -mjson.tool
    {
        "count": 1,
        "message": "Success",
        "status": 200,
        "values": [
            {
                "created": 1494527723,
                "description": "MySQL Configuration",
                "id": "mysql_database",
                "name": "MySQL Database",
                "properties": {
                    "hostaname": "localhost",
                    "port": 3306.0
                },
                "type": "DATABASE",
                "updated": 1494527723
            }
        ]
    }

Cloning connection
~~~~~~~~~~~~~~~~~~

::

    curl -s "http://localhost:11015/v3/namespaces/default/apps/dataprep/services/service/methods/connections/mysql_database/clone" | python -mjson.tool
    {
        "count": 1,
        "message": "Success",
        "status": 200,
        "values": [
            {
                "created": 1494528015,
                "description": "MySQL Configuration",
                "name": "MySQL Database_Clone",
                "properties": {
                    "hostaname": "localhost",
                    "port": 3306.0
                },
                "type": "DATABASE",
                "updated": 1494528015
            }
        ]
    }

Fetch only properties
~~~~~~~~~~~~~~~~~~~~~

::

    curl -s "http://localhost:11015/v3/namespaces/default/apps/dataprep/services/service/methods/connections/mysql_database/properties" | python -mjson.tool
    {
        "count": 1,
        "message": "Success",
        "status": 200,
        "values": [
            {
                "hostaname": "localhost",
                "port": 3306.0
            }
        ]
    }

Adding new property or updating existing property
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    curl -X PUT -s "http://localhost:11015/v3/namespaces/default/apps/dataprep/services/service/methods/connections/mysql_database/properties?key=ssl&value=true" | python -mjson.tool
    {
        "count": 1,
        "message": "Success",
        "status": 200,
        "values": [
            {
                "hostaname": "localhost",
                "port": 3306.0,
                "ssl": "true"
            }
        ]
    }
