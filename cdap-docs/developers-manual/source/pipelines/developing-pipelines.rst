.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016-2017 Cask Data, Inc.

.. _cdap-pipelines-developing-pipelines:

====================
Developing Pipelines
====================

.. highlight:: console

This section is intended for developers creating pipelines using command line tools. Users of
pipelines should refer to the documentation on :ref:`creating pipelines
<cdap-pipelines-creating-pipelines>`.

As mentioned in :ref:`creating pipelines <cdap-pipelines-creating-pipelines>`, there are two
different methods for creating pipeline applications:

1. Using the CDAP Studio
#. Using command line tools (such as ``curl``, the CDAP CLI, or the CDAP UI)

Using comamnd line tools, pipelines can be created using the :ref:`Lifecycle RESTful API
<http-restful-api-lifecycle-create-app>`, the :ref:`CDAP CLI <cli>`, or the :ref:`CDAP UI
<cdap-ui>`. 

In order to create a pipeline application, a pipeline configuration (either as a file or
in-memory) is required that specifies the configuration of plugins to be used along with
their properties. (In the CDAP Studio, the user interface prompts you for the required
information.)

This section describes how to create such a configuration, and various aspects to be
considered in the design of pipelines.

.. _cdap-pipelines-developing-pipelines-configuration-file-format:

Pipeline Configuration File Format
==================================
The configuration file format is JSON, containing these objects at the top level:

.. list-table::
   :widths: 10 10 80
   :header-rows: 1

   * - Object
     - Type
     - Description
   * - ``name``
     - ``String``
     - Name of the pipeline
   * - ``artifact``
     - ``Object``
     - Specifies the artifact used to create the pipeline
   * - ``config``
     - ``Object``
     - Configuration of the pipeline
   * - ``__ui__``
     - ``Object``
     - Specifies the UI display of the pipeline

The first three are required, while the ``__ui__`` is optional. It is added by the CDAP Application
Studio to store the display of the pipeline in the UI. The format of the ``__ui__`` object
is not described here, as it is internal to the CDAP Studio UI and subject to change.

Example (in JSON format):

.. container:: highlight

  .. parsed-literal::

    {
      "name": "MyPipeline",
      "artifact": {
        "name": "cdap-data-pipeline",
        "version": "|release|",
        "scope": "system"
      },
      "config": {
        . . .
        "connections": [ . . . ],
        "engine": "mapreduce",
        "postActions": [ . . . ],
        "stages": [ . . . ],
        "schedule": "0 \* \* \* \*",
      },
      "__ui__": {
        . . .
      }
    }


Artifact Format
---------------
The format of an ``artifact`` object:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Key
     - Description
   * - ``name``
     - Name of the artifact
   * - ``version``
     - Allowable version(s) of the artifact
   * - ``scope``
     - Scope of the artifact, one of either ``user`` or ``system``


Configuration Format
--------------------
The format of the ``config`` object:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Key
     - Description
   * - ``connections``
     - List of ``connection`` objects
   * - ``engine``
     - One of "mapreduce" or "spark", used only in batch pipelines
   * - ``postActions``
     - List of ``postaction`` (post-run action) objects; optional, can be an empty list
   * - ``instances``
     - Positive integer of the number of worker instances, defaults to 1 if not specified,
       used only in real-time pipelines
       
   * - ``resources``
     - A map defining the resources to be used for worker instance of pipeline, such as
       ``{ "memoryMB": 2048, "virtualCores": 1 }``

   * - ``driverResources``
     - A map defining the resources to be used for a database driver, such as username or password      
       
       
   * - ``schedule``
     - String in ``cron`` file format, used only in batch pipelines
   * - ``stages``
     - List of ``stage`` objects

The format of the ``post-action`` object is identical to that of a ``stage`` object, as
they are both a JSON representation of a plugin. However, only plugins of type
``postaction`` can be included in the list for ``postActions``. Each ``postaction`` object
must have a unique name.

The format of a ``connection`` object:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Key
     - Description
   * - ``from``
     - String name of ``stage`` connected from
   * - ``to``
     - String name of ``stage`` connected to

The format of ``stage`` and ``postaction`` objects:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Key
     - Description
   * - ``name``
     - String name; these must be unique to distinguish stages in the ``connections`` object and
       post-actions in the ``postActions`` list
   * - ``plugin``
     - Plugin object
   * - ``errorDatasetName``
     - Name of a dataset that any error messages will be written to; used by
       :ref:`validating transform <cdap-pipelines-running-pipelines-error-record-handling>`
       stages

The format of a ``plugin`` object:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Key
     - Description
   * - ``name``
     - String name identifying the plugin
   * - ``type``
     - String type of plugin
   * - ``artifact``
     - Optional artifact object; if not included or defined, the highest version available is used
   * - ``properties``
     - Map of properties, contents of which are determined by the particular plugin used


.. _cdap-pipelines-developing-pipelines-creating-batch:

Creating a Batch Pipeline
=========================
With a CDAP batch pipeline, a ``schedule`` property is required with a ``cron`` entry
specifying the frequency of the batch job run, such as every day or every hour.

For example, this JSON (when in a file such as ``config.json``) provides a
configuration for a batch pipeline that runs every minute, reading data from a stream
*myStream* and writing to a dataset (``Table``) called *myTable*, without any transformations;
when the run completes, a post-run action send an email indicating that the run has completed:

.. container:: highlight

  .. parsed-literal::
  
    {
      "name": "streamETLApp",
      "artifact": {
        "name": "cdap-data-pipeline",
        "version": "|release|",
        "scope": "system"
      },
      "config": {
        "schedule": "\* \* \* \* \*",
        "engine": "mapreduce",
        "postActions": [
          {
            "name": "Email",
            "plugin": {
              "name": "Email",
              "type": "postaction",
              "artifact": {
                "name": "core-plugins",
                "version": "|cdap-pipelines-version|",
                "scope": "SYSTEM"
              },
              "properties": {
                "runCondition": "completion",
                "includeWorkflowToken": "false",
                "recipients": "users@example.com",
                "sender": "admin@example.com",
                "subject": "Post-action Status",
                "message": "Completed run."
              }
            }
          }
        ],
        "stages": [
          {
            "name": "streamSource",
            "plugin": {
              "name": "Stream",
              "type": "batchsource",
              "artifact": {
                "name": "core-plugins",
                "version": "|cdap-pipelines-version|",
                "scope": "system"
              },
              "properties": {
                "name": "myStream",
                "duration": "1m",
                "format": "text",
                "schema": "{\\"type\\":\\"record\\",\\"name\\":\\"etlSchemaBody\\",\\"fields\\":[{\\"name\\":\\"body\\",\\"type\\":\\"string\\"}]}"
              }
            }
          },
          {
            "name": "tableSink",
            "plugin": {
              "name": "Table",
              "type": "batchsink",
              "artifact": {
                "name": "core-plugins",
                "version": "|cdap-pipelines-version|",
                "scope": "system"
              },              
              "properties": {
                "name": "myTable",
                "schema": "{\\"type\\":\\"record\\",\\"name\\":\\"etlSchemaBody\\",\\"fields\\":[{\\"name\\":\\"ts\\",\\"type\\":\\"long\\"},{\\"name\\":\\"body\\",\\"type\\":\\"string\\"}]}",
                "schema.row.field": "ts"
              }
            }
          }
        ],
        "connections": [
          {
            "from": "streamSource",
            "to": "tableSink"
          }
        ]
      }
    }

This pipeline launches a MapReduce program that runs every minute, reads data from the
``Stream`` *myStream* and writes to a ``Table`` *myTable*. A *Table Sink* needs a row key field to
be specified and can use the timestamp of a stream event for that.

A pipeline configuration (the ``config`` object) consists of stages, connections and other
properties. The stages consist of a single source, zero (or more) transforms, and one (or
more) sink(s). Each of these stages is identified by a unique name and the plugin to be used.

A ``plugin`` object is specified by a plugin-name, type, a properties map and can
(optionally) specify the artifact. If the artifact is not specified, the pipeline will
choose the artifact with the highest version.

The connections field in the configuration defines the connections between the stages,
using the unique names of the stages. The pipeline defined by these connections must be a
directed acyclic graph (or DAG).

To create this pipeline, called *streamETLApp*, you can use either an HTTP RESTful API or
the CDAP CLI.

- Using the :ref:`Lifecycle RESTful API <http-restful-api-lifecycle-create-app>`:

  .. tabbed-parsed-literal::

    $ curl -w"\n" -X PUT localhost:11015/v3/namespaces/default/apps/streamETLApp \
        -H "Content-Type: application/json" -d @config.json
        
    Deploy Complete

- Using the :ref:`CDAP CLI <cli>`:

  .. tabbed-parsed-literal::

    $ cdap cli create app streamETLApp cdap-etl-batch |release| system <path-to-config.json>
    
    Successfully created application

where, in both cases, ``config.json`` is the file that contains the pipeline configuration shown above.

.. _cdap-pipelines-developing-pipelines-creating-real-time:

Creating a Real-Time Pipeline
=============================
With a CDAP real-time pipeline, a ``batchInterval`` property is required
specifying the frequency at which the sources will create new micro batches of data.
The ``batchInterval`` must be a number followed by a time unit, with 's' for seconds,
'm' for minutes, and 'h' for hours. For example, a value of '10s' will be interpreted
as ten seconds. This means that every ten seconds, a new micro batch of data will be generated. 

To create a real-time pipeline that reads from a source such as Twitter and writes to a
CDAP Table after performing a projection transformation, you can use a configuration such as:

.. container:: highlight

  .. parsed-literal::
  
    {
      "name": "twitterToStream",
      "artifact": {
        "name": "cdap-data-streams",
        "version": "|release|",
        "scope": "system"
      },
      "config": {
        "batchInterval": "10s",
        "stages": [
          {
            "name": "twitterSource",
            "plugin": {
              "name": "Twitter",
              "type": "streamingsource",
              "artifact": {
                "name": "spark-plugins",
                "version": "|cdap-pipelines-version|",
                "scope": "system"
              },
              "properties": {
                "referenceName": "xxx",
                "AccessToken": "xxx",
                "AccessTokenSecret": "xxx",
                "ConsumerKey": "xxx",
                "ConsumerSecret": "xxx"
              }
            }
          },
          {
            "name": "dropProjector",
            "plugin": {
              "name": "Projection",
              "type": "transform",
              "artifact": {
                "name": "core-plugins",
                "version": "|cdap-pipelines-version|",
                "scope": "system"
              },
              "properties": {
                "drop": "lang,time,favCount,source,geoLat,geoLong,isRetweet",
                "rename": "message:tweet,rtCount:retCount"
              }
            }
          },
          {
            "name": "tableSink",
            "plugin": {
              "name": "Table",
              "type": "batchsink",
              "artifact": {
                "name": "core-plugins",
                "version": "|cdap-pipelines-version|",
                "scope": "system"
              },              
              "properties": {
                "name": "myTable",
                "schema": "{\\"type\\":\\"record\\",\\"name\\":\\"etlSchemaBody\\",\\"fields\\":[{\\"name\\":\\"id\\",\\"type\\":\\"long\\"},{\\"name\\":\\"tweet\\",\\"type\\":\\"string\\"},{\\"name\\":\\"retCount\\",\\"type\\":\\"int\\"}]}",
                "schema.row.field": "id"
              }
            }
          }
        ],
        "connections": [
          {
            "from": "twitterSource",
            "to": "dropProjector"
          },
          {
            "from": "dropProjector",
            "to": "tableSink"
          }
        ]
      }
    }

In the example code above, we will use a *ProjectionTransform* (a type of transform) to
drop and rename selected columns in the incoming data. A *TableSink* in the final step
requires a schema property that it will use to set the columns and row field for
writing the data to a CDAP Table.

Non-linear Executions in Pipelines
==================================
CDAP pipelines supports directed acyclic graphs in pipelines, which allows for the non-linear
execution of pipeline stages.

Fork in Pipeline
----------------
In this example, a pipeline reads from the stream ``purchaseStats``. It writes the stream
events to the table ``replicaTable``, while at the same time it writes just the
``userIds`` to the ``usersTable`` when a user's purchase price is greater than 1000. This
filtering logic is applied by using an included script in the step
``spendingUsersScript``:

.. image:: _images/fork-in-pipeline.png
   :width: 6in
   :align: center

.. container:: highlight

  .. parsed-literal::

    {
      "name": "forkedPipeline",
      "artifact": {
        "name": "cdap-data-pipeline",
        "version": "|release|",
        "scope": "SYSTEM"
      },
      "config": {
        "schedule": "\* \* \* \* \*",
        "engine": "mapreduce",
        "postActions": [],
        "stages": [
          {
            "name": "purchaseStats",
            "plugin": {
              "name": "Stream",
              "type": "batchsource",
              "artifact": {
                "name": "core-plugins",
                "version": "|cdap-pipelines-version|",
                "scope": "SYSTEM"
              },
              "properties": {
                "name": "testStream",
                "duration": "1d",
                "format": "csv",
                "schema": "{\\"type\\":\\"record\\",\\"name\\":\\"etlSchemaBody\\",\\"fields\\":[{\\"name\\":\\"userId\\",\\"type\\":\\"string\\"},{\\"name\\":\\"purchaseItem\\",\\"type\\":\\"string\\"},{\\"name\\":\\"purchasePrice\\",\\"type\\":\\"long\\"}]}"
              }
            }
          },
          {
            "name": "replicaTable",
            "plugin": {
              "name": "Table",
              "type": "batchsink",
              "artifact": {
                "name": "core-plugins",
                "version": "|cdap-pipelines-version|",
                "scope": "SYSTEM"
              },
              "properties": {
                "name": "replicaTable",
                "schema": "{\\"type\\":\\"record\\",\\"name\\":\\"etlSchemaBody\\",\\"fields\\":[{\\"name\\":\\"userId\\",\\"type\\":\\"string\\"},{\\"name\\":\\"purchaseItem\\",\\"type\\":\\"string\\"},{\\"name\\":\\"purchasePrice\\",\\"type\\":\\"long\\"}]}",
                "schema.row.field": "userId"
              }
            }
          },
          {
            "name": "usersTable",
            "plugin": {
              "name": "Table",
              "type": "batchsink",
              "artifact": {
                "name": "core-plugins",
                "version": "|cdap-pipelines-version|",
                "scope": "SYSTEM"
              },
              "properties": {
                "name": "targetCustomers",
                "schema": "{\\"type\\":\\"record\\",\\"name\\":\\"etlSchemaBody\\",\\"fields\\":[{\\"name\\":\\"userId\\",\\"type\\":\\"string\\"}]}",
                "schema.row.field": "userId"
              }
            }
          },
          {
            "name": "spendingUsersScript",
            "plugin": {
              "name": "JavaScript",
              "type": "transform",
              "artifact": {
                "name": "core-plugins",
                "version": "|cdap-pipelines-version|",
                "scope": "SYSTEM"
              },
              "properties": {
                "script": "function transform(input, emitter, context) {\\n  if (input.purchasePrice > 1000) {\\n  emitter.emit(input);\\n  }  \\n}"
              }
            }
          }
        ],
       "connections": [
          {
            "from": "purchaseStats",
            "to": "replicaTable"
          },
          {
            "from": "purchaseStats",
            "to": "spendingUsersScript"
          },
          {
            "from": "spendingUsersScript",
            "to": "usersTable"
          }
        ]
      }
    }

Pipeline connections can be configured to fork from a stage, with the output of the stage
sent to two or more configured stages; in the above example, the output record from
``purchaseStats`` will be sent to both ``replicaTable`` and ``spendingUsersScript`` stages.

Merging Stages in Pipeline
--------------------------
Forked transform stages can merge together at a transform or a sink stage.

A merge does not join, or modify records in any way. It simply means that multiple stages
can write to the same stage. The only requirement is that all stages must output records
of the same schema to the merging stage. Note that the order of records sent from the
forked stages to the merging stage will not be defined.

In this next example, ``purchaseStream`` has purchase data with fields ``userid``,
``item``, ``count``, and ``price``. The stream events source stage ``purchaseStream``
forks, and records are sent to both of the transforms ``userRewards`` and ``itemRewards``.

The ``userRewards`` transform script looks up valued customers in the table
``hvCustomers``, to check if ``userid`` is a valued customer and assigns higher rewards if
they are. After calculating the rewards, this transform sends an output record in the
format ``userid(string), rewards(double)``.

The ``itemRewards`` transform script awards higher rewards for bulk purchases and sends
output records in the same format, ``userid(string), rewards(double)``.

The rewards records are merged at the sink ``rewardsSink``; note that the incoming schema
from the transforms ``userRewards`` and ``itemRewards`` are the same, and that the order
of received records will vary.

.. image:: _images/merge-in-pipeline.png
   :width: 8in
   :align: center

.. container:: highlight

  .. parsed-literal::

    {
      "name": "mergedPipeline",
      "artifact": {
          "name": "cdap-data-pipeline",
          "version": "|release|",
          "scope": "SYSTEM"
      },
      "config": {
        "schedule": "\* \* \* \* \*",
        "engine": "mapreduce",
        "postActions": [],
        "stages": [
          {
            "name": "purchaseStream",
            "plugin": {
              "name": "Stream",
              "type": "batchstream",
              "artifact": {
                "name": "core-plugins",
                "version": "|cdap-pipelines-version|",
                "scope": "SYSTEM"
              },
              "properties": {
                "format": "csv",
                "schema": "{
                  \"type\":\"record\",
                  \"name\":\"etlSchemaBody\",
                  \"fields\":[
                    {\"name\":\"userid\",\"type\":\"string\"},
                    {\"name\":\"item\",\"type\":\"string\"},
                    {\"name\":\"count\",\"type\":\"int\"},
                    {\"name\":\"price\",\"type\":\"long\"}
                  ]
                }",
                "name": "purchases",
                "duration": "1d"
              }
            }
          },
          {
            "name": "userRewards",
            "plugin": {
              "name": "Script",
              "type": "transform",
              "artifact": {
                "name": "core-plugins",
                "version": "|cdap-pipelines-version|",
                "scope": "SYSTEM"
              },
              "properties": {
                "script": "function transform(input, context) {
                  var rewards = 5;
                  if (context.getLookup('hvCustomers').lookup(input.userid) !== null) {
                    context.getLogger().info(\"user \" + input.userid + \" is a valued customer\");
                    rewards = 100;
                  } else {
                    context.getLogger().info(\"user \" + input.userid + \" is not a valued customer\");
                  }
                  return {'userid': input.userid, 'rewards': rewards};
                }",
                "schema": "{
                  \"type\":\"record\",
                  \"name\":\"etlSchemaBody\",
                  \"fields\":[
                    {\"name\":\"userid\",\"type\":\"string\"},
                    {\"name\":\"rewards\",\"type\":\"double\"}
                  ]
                }",
                "lookup": "{\"tables\":{\"hvCustomers\":{\"type\":\"DATASET\",\"datasetProperties\":{}}}}"
              }
            }
          },
          {
            "name": "itemRewards",
            "plugin": {
              "name": "Script",
              "type": "transform",
              "artifact": {
                "name": "core-plugins",
                "version": "|cdap-pipelines-version|",
                "scope": "SYSTEM"
              },
              "properties": {
                "script": "function transform(input, context) {
                  var rewards = 5;
                  if (input.count > 20) {
                    rewards = 50;
                  }
                  return {'userid':input.userid, 'rewards':rewards};
                }",
                "schema": "{
                  \"type\":\"record\",
                  \"name\":\"etlSchemaBody\",
                  \"fields\":[
                    {\"name\":\"userid\",\"type\":\"string\"},
                    {\"name\":\"rewards\",\"type\":\"double\"}
                  ]
                }"
              }
            }
          },
          {
            "name": "rewardsSink",
            "plugin": {
              "name": "TPFSAvro",
              "type": "batchsink",
              "artifact": {
                "name": "core-plugins",
                "version": "|cdap-pipelines-version|",
                "scope": "SYSTEM"
              },
              "properties": {
                "schema": "{
                  \"type\":\"record\",
                  \"name\":\"etlSchemaBody\",
                  \"fields\":[
                    {\"name\":\"userid\",\"type\":\"string\"},
                    {\"name\":\"rewards\",\"type\":\"double\"}
                  ]
                }"
              }
            }
          }
        ],
        "connections": [
          {
            "from": "purchaseStream",
            "to": "userRewards"
          },
          {
            "from": "userRewards",
            "to": "rewardsSink"
          },
          {
            "from": "purchaseStream",
            "to": "itemRewards"
          },
          {
            "from": "itemRewards",
            "to": "rewardsSink"
          }
        ],
      }
    }


Sample Pipeline Configurations
==============================

Database Source and Sink
------------------------
Sample configuration for using a *Database Source* and a *Database Sink*:

.. container:: highlight

  .. parsed-literal::
  
    {
      "artifact": {
        "name": "cdap-data-pipeline",
        "version": "|version|",
        "scope": "system"
      },
      "config": {
        "schedule": "\* \* \* \* \*",
        "stages": [
          {
            "name": "databaseSource",
            "plugin": {
              "name": "Database",
              "type": "batchsource",
              "artifact": {
                "name": "core-plugins",
                "version": "|cdap-pipelines-version|",
                "scope": "SYSTEM"
              },
              "properties": {
                "importQuery": "select id,name,age from my_table",
                "countQuery": "select count(id) from my_table",
                "connectionString": "\jdbc:mysql://localhost:3306/test",
                "tableName": "src_table",
                "user": "my_user",
                "password": "my_password",
                "jdbcPluginName": "jdbc_plugin_name_defined_in_jdbc_plugin_json_config",
                "jdbcPluginType": "jdbc_plugin_type_defined_in_jdbc_plugin_json_config"
              }
            }
          },
          {
            "name": "databaseSink",
            "plugin": {
              "name": "Database",
              "type": "batchsink",
              "artifact": {
                "name": "core-plugins",
                "version": "|cdap-pipelines-version|",
                "scope": "SYSTEM"
              },
              "properties": {
                "columns": "id,name,age",
                "connectionString": "\jdbc:mysql://localhost:3306/test",
                "tableName": "dest_table",
                "user": "my_user",
                "password": "my_password",
                "jdbcPluginName": "jdbc_plugin_name_defined_in_jdbc_plugin_json_config",
                "jdbcPluginType": "jdbc_plugin_type_defined_in_jdbc_plugin_json_config"
              }
            }
          }
        ],
        "connections": [
          {
            "from": "databaseSource",
            "to": "databaseSink"
          }
        ]
      }
    }

  
Kafka Source
------------
A Kafka cluster needs to be available, and certain minimum properties specified when
creating the source:

.. container:: highlight

  .. parsed-literal::
  
    {
      "name": "KafkaPipeline",
      "artifact": {
        "name": "cdap-data-streams",
        "version": "|version|",
        "scope": "system"
      },
      "config": {
        "batchInterval": "1s",
        "connections": [
          {
            "from": "kafkaSource",
            "to": "tableSink"
          }
        ],
        "stages": [
          {
            "name": "kafkaSource",
            "plugin": {
              "name": "Kafka",
              "type": "streamingsource",
              "artifact": {
                "name": "spark-plugins",
                "version": "|cdap-pipelines-version|",
                "scope": "SYSTEM"
              },
              "properties": {
                "referenceName": "purchasesTopic",
                "brokers": "localhost:9092",
                "topics": "purchases",
                "format": "csv",
                "schema": "{\\"type\\":\\"record\\",\\"name\\":\\"etlSchemaBody\\",\\"fields\\":[{\\"name\\":\\"id\\",\\"type\\":\\"long\\"},{\\"name\\":\\"customer\\",\\"type\\":\\"string\\"},{\\"name\\":\\"item\\",\\"type\\":\\"string\\"}]}"
              }
            }
          },
          {
            "name": "tableSink",
            "plugin": {
              "name": "Table",
              "type": "batchsink",
              "artifact": {
                "name": "core-plugins",
                "version": "|cdap-pipelines-version|",
                "scope": "system"
              },              
              "properties": {
                "name": "myTable",
                "schema": "{\\"type\\":\\"record\\",\\"name\\":\\"etlSchemaBody\\",\\"fields\\":[{\\"name\\":\\"id\\",\\"type\\":\\"long\\"},{\\"name\\":\\"customer\\",\\"type\\":\\"string\\"},{\\"name\\":\\"item\\",\\"type\\":\\"string\\"}]}"
                "schema.row.field": "id"
              }
            }
          }
        ]
      }
    }


Prebuilt JARs
-------------
In a case where you'd like to use prebuilt third-party JARs (such as a JDBC driver) as a
plugin, please refer to the section on :ref:`Deploying Third-Party Jars
<cdap-pipelines-plugin-management-third-party-plugins>`. 
