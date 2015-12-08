.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _cdap-apps-etl-creating:

===========================
Creating an ETL Application
===========================

.. highlight:: console

Introduction
============
Applications can be created using the :ref:`Lifecycle RESTful API <http-restful-api-lifecycle-create-app>`,
the :ref:`CDAP CLI <cli>`, or the :ref:`CDAP UI <cdap-ui>`. In order to create an application,
an application configuration is required that specifies the source, transformations and sinks
along with their properties.  (In the CDAP UI, the interface prompts you for the required
information.)


.. _cdap-apps-etl-configuration-file-format:

Configuration File Format
=========================

Creating an ETL Batch Application
---------------------------------

With an ETL Batch Application, it requires a ``schedule`` property with a cron entry
specifying the frequency of the Batch job run, such as every day or every hour.

For example, this JSON (when in a file such as ``config.json``) provides a
configuration for a Batch Application that runs every minute, reading data from a stream
*myStream* and writing to a dataset (Table) called *myTable*, without any transformations:

.. container:: highlight

  .. parsed-literal::
    {
      "artifact": {
        "name": "cdap-etl-batch",
        "version": "|version|",
        "scope": "system"
      },
      "config": {
        "schedule": "\* \* \* \* \*",
        "source": {
          "name": "Stream",
          "properties": {  
            "name": "myStream",
            "duration": "1m"
          }
        },
        "transforms": [ ],
        "sinks": [
          {
            "name": "Table",
            "properties": {
              "name": "myTable",
              "schema.row.field": "ts"
            }
          }
        ]
      }
    }

The application launches a MapReduce program that runs every minute, reads data from the
stream *myStream* and writes to a Table *myTable*. A Table Sink needs a row key field to
be specified and can use the timestamp of a Stream event for that.

To create this application, called *streamETLApp*:

- Using the :ref:`Lifecycle RESTful API <http-restful-api-lifecycle-create-app>`::

    $ curl -w'\n' -X PUT localhost:10000/v3/namespaces/default/apps/streamETLApp \
        -H 'Content-Type: application/json' -d @config.json 

- Using :ref:`CDAP CLI <cli>`:

  .. container:: highlight

    .. parsed-literal::
      |$| cdap-cli.sh create app streamETLApp cdap-etl-batch |version| system <path-to-config.json>

where ``config.json`` is the file that contains the application configuration.


.. highlight:: console

Creating an ETL Real-Time Application
-------------------------------------

This next configuration creates a real-time application that reads from Twitter and writes to a
stream after performing a projection transformation:

.. container:: highlight

  .. parsed-literal::
    {
      "artifact": {
        "name": "cdap-etl-realtime",
        "version": "|version|",
        "scope": "system"
      },
      "config": {
        "instances": 1,
        "source": {
          "name": "Twitter",
          "properties": {  
            "AccessToken": "xxx",
            "AccessTokenSecret": "xxx",
            "ConsumerKey": "xxx",
            "ConsumerSecret": "xxx"                                         
          }
        },
        "transforms": [
          {
            "name": "Projection",
            "properties": {
              "drop": "lang,time,favCount,source,geoLat,geoLong,isRetweet"
            }
          }
        ],
        "sinks": [
          {
            "name": "Stream",
            "properties": {
              "name": "twitterStream",
              "body.field": "tweet"
            }
          }
        ]
      }
    }


An ETL Real-Time Application expects an instance property that will create *N* instances
of the worker that run concurrently. In Standalone CDAP mode, this is implemented as
multiple threads; in Distributed CDAP mode, it will create different YARN containers. The
number of worker instances of a real-time application should not (in general) be changed
during runtime. If the number of instances needs to be changed, the worker must be
stopped, then the application configuration must be updated to the new number of instances.

The ``instances`` property value needs to be greater than 0. Note that the ``instance``
property replaces the ``schedule`` property of an ETL Batch Application.

In the example code above, we will use a *ProjectionTransform* (a type of Transform) to drop certain
columns in the incoming data. A *StreamSink* in the final step needs a data field property
that it will use as the content for the data to be written. 

Sample Application Configurations
---------------------------------

**Database:** Sample config for using a Database Source and a Database Sink:

.. container:: highlight

  .. parsed-literal::
    {
      "artifact": {
        "name": "cdap-etl-batch",
        "version": "|version|",
        "scope": "system"
      },
      "config": {
        "schedule": "\* \* \* \* \*",
        "source": {
          "name": "Database",
          "properties": {
            "importQuery": "select id,name,age from my_table",
            "countQuery": "select count(id) from my_table",
            "connectionString": "jdbc:mysql://localhost:3306/test",
            "tableName": "src_table",
            "user": "my_user",
            "password": "my_password",
            "jdbcPluginName": "jdbc_plugin_name_defined_in_jdbc_plugin_json_config",
            "jdbcPluginType": "jdbc_plugin_type_defined_in_jdbc_plugin_json_config"
          }
        },
        "sinks": [
          {
            "name": "Database",
            "properties": {
              "columns": "id,name,age",
              "connectionString": "jdbc:mysql://localhost:3306/test",
              "tableName": "dest_table",
              "user": "my_user",
              "password": "my_password",
              "jdbcPluginName": "jdbc_plugin_name_defined_in_jdbc_plugin_json_config",
              "jdbcPluginType": "jdbc_plugin_type_defined_in_jdbc_plugin_json_config"
            }
          }
        ],
        "transforms": [ ]
      }
    }
  
**Kafka:** A Kafka cluster needs to be setup, and certain minimum properties specified when
creating the source:

.. container:: highlight

  .. parsed-literal::
    {
      "artifact": {
        "name": "cdap-etl-realtime",
        "version": "|version|",
        "scope": "system"
      },
      "config": {
        "instances": 1,
        "source": {
          "name": "Kafka",
          "properties": {
            "kafka.partitions": "1",
            "kafka.topic": "test",
            "kafka.brokers": "localhost:9092"
          }
        },
        "sinks": [
          {
            "name": "Stream",
            "properties": {
              "name": "myStream",
              "body.field": "message"
            }
          }
        ],
        "transforms": [ ]
      }
    }


**Prebuilt JARs:** In a case where you'd like to use prebuilt third-party JARs (such as a
JDBC driver) as a plugin, please refer to the section on :ref:`Using Third-Party Jars
<cdap-apps-third-party-jars>`. 
