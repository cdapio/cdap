.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _users-etl-creating:

=======================
Creating An ETL Adapter
=======================

.. highlight:: console

Introduction
============

CDAP is shipped with two built-in Application Templates: **ETL Batch** and **ETL
Realtime**. These templates can be used to create ETL Adapters for common ETL requirements.

An ETL Adapter is composed from three different *Plugins*: 

- Source
- Transform (short for *Transformation*)
- Sink

Each ETL Adapter can have one *Source*, zero or more *Transforms* and one *Sink*. 
A plugins can accept a set of properties. For each ETL plugin, certain properties
might be required with the remainder optional.


Configuration File Format
=========================

Using the ETL Batch Template
----------------------------

For the ETL Realtime Template, it requires a ``schedule`` property with a cron entry
specifying the frequency of the Batch job run, such as every day or every hour.

::

  {
    "template":"etlBatch",
    "description":"Description of the Adapter",
    "config":{
               "schedule":"* * * * *",
               "source":{
                          "name":"SourceName",
                          "properties":{  
                                         "key1":"val1",
                                         "key2":"val2",
                                         ...
                                       }
                        },
               "transforms":[
                              {
                                "name":"Transform1"
                                "properties":{
                                               "key":"val",
                                               ...
                                             }
                              },
                              {
                                "name":"Transform2"
                                "properties":{
                                                "key":"val",
                                                ...
                                             }
                              },
                              ...
                            ],
               "sink":{
                        "name":"SinkName",
                        "properties":{
                                       "key":"val",
                                       ...
                                     }
                      }
             }
  }


Using the ETL Realtime Template
-------------------------------

The ETL Realtime Template expects an instance property that will create *N* instances of the
adapter that run concurrently. In Standalone CDAP mode, this is implemented as multiple threads;
while in Distributed CDAP mode, it will create different YARN containers.

The ``instances`` property value needs to be greater than 0. Note that the ``instance``
property replaces the ``schedule`` property of the ETL Batch Template.

::

  {
    "template":"etlRealtime",
    "description":"Description of the Adapter",
    "config":{
               "instances":"1",
               "source":{
                          "name":"SourceName",
                          "properties":{  
                                         "key1":"val1",
                                         "key2":"val2",
                                         ...
                                       }
                        },
               "transforms":[
                              {
                                "name":"Transform1"
                                "properties":{
                                               "key":"val",
                                               ...
                                             }
                              },
                              {
                                "name":"Transform2"
                                "properties":{
                                                "key":"val",
                                                ...
                                             }
                              },
                              ...
                            ],
               "sink":{
                        "name":"SinkName",
                        "properties":{
                                       "key":"val",
                                       ...
                                     }
                      }
             }
  }


Example ETL Adapters
====================

Example Batch Adapter
---------------------

In this example, we look at creating an Adapter of type ETL Batch that reads from a
CDAP Stream and writes to a CDAP Table and that runs every 30 mins. This will launch a
MapReduce program that runs every 30 minutes and reads data from the Stream *myStream* and
writes to a Table *myTable*. A Table Sink needs a row key field to be specified and can use the
timestamp of a Stream event for that. (It's explained below how to find out which properties
a plugin such as a Sink requires.)


::

  {
    "template":"etlBatch",
    "description":"Stream to Table example using Stream event’s timestamp as the Table row key",
    "config":{
               "schedule":"*/30 * * * *",
               "source":{
                          "name":"Stream",
                          "properties":{  
                                         "name":"myStream",
                                         "duration":"30m"
                                       }
                        },
               "sink":{
                        "name":"Table",
                        "properties":{
                                       "name":"myTable",
                                       "schema.row.field":"ts"
                                     }
                      }
             }
  }

Example Realtime Adapter
------------------------

In this example, we look at creating an Adapter of type ETL Realtime that receives data from
Twitter, performs processing on the data, and then writes it to a CDAP Stream *twitterStream*.

In this case, we will use a *ProjectionTransform* (a type of Transform) to rename a field in the incoming data
from "message" to "tweet". Stream Sink needs a data field property that it will use as the
content for the data to be written. The instances property is set to 1 and thus it will
launch a thread or a yarn container (depending on the runtime). The number of instances
of a Realtime Adapter cannot be changed during runtime. 

(It's explained below how to find out which properties a plugin requires.)
 

.. highlight:: console

::

  {
    "template":"etlRealtime",
    "description":"Twitter to Stream example that renames the 'message' field name to 'tweet',
    "config":{
               "instances":"1",
               "source":{
                          "name":"Twitter",
                          "properties":{  
                                         "AccessToken":"xxx",
                                         "AccessTokenSecret":"xxx",
                                         "ConsumerKey":"xxx",
                                         "ConsumerSecret":"xxx"                                         
                                       }
                        },
               "transforms":[
                              {
                                "name":"Projection"
                                "properties":{
                                               "rename":"message:tweet"
                                             }
                              }
                            ],
               "sink":{
                        "name":"Stream",
                        "properties":{
                                       "name":"twitterStream",
                                       "body.field":"tweet"
                                     }
                      }
             }
  }

Configuring ETL Adapters and Plugins
====================================

.. highlight:: console

In order to configure an ETL Adapter, you’ll need information about the different ETL
plugins you are using: which sources, transforms, and sinks are available, and what
properties need to be specified for each of them.

This information can be retrieved from the platform using the CDAP ETL Adapter HTTP
RESTful API. You can access this with the CDAP CLI (Command Line Interface), ``curl``
calls on a command line, or [through the CDAP UI].

In each of the following endpoints, ``<template-id>`` refers to either ``etlBatch`` or
``etlRealtime``. For example, if you fetch the information on sources for the
``etlRealtime`` template, it will provide information about all the realtime sources that
are available in the platform.

If you have added your own Application Templates [link] or Plugins [link], then in the endpoints
described below, you can substitute your Template IDs and Types as appropriate. 

These endpoints are specific to ETL Application Templates and their Plugins. Endpoints
that are applicable to all plugins [link] (including the ones you might have created) are
documented in the CDAP Reference Manual, HTTP RESTful API [link].

Retrieving Available Plugin Types
---------------------------------

To retrieve the available Plugin types (extensions) for an Application Template, use::

  GET <base-url>/templates/<template-id>
  
  
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<template-id>``
     - Template ID, one of either ``etlBatch`` or ``etlRealtime``


Retrieving Available Plugins
----------------------------

To retrieve the available Plugins of a given type (extension) for an Application Template, use::

  GET <base-url>/templates/<template-id>/extensions/<type>
  
  
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<template-id>``
     - Template ID, one of either ``etlBatch`` or ``etlRealtime``
   * - ``<type>``
     - Plugin type, one of either ``source``, ``transform``, or ``sink``


Retrieving Plugin Details
-------------------------

To retrieve the Plugin details for an Application Template, use::

  GET <base-url>/templates/<template-id>/extensions/<type>/plugins/<plugin-id>
  
  
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<template-id>``
     - Template ID, one of either ``etlBatch`` or ``etlRealtime``
   * - ``<type>``
     - Plugin type, one of either ``source``, ``transform``, or ``sink``
   * - ``<plugin-id>``
     - Plugin ID (name), as obtained from a previous call to retrieve the available plugins


Examples
--------

::

  # Batch

  # Retrieving the available types of plugins of the ETL Batch Application Template
  $ curl -w'\n' -X GET 'http://localhost:10000/v3/templates/etlBatch'

  {"extensions":["sink","source","transform"],"name":"etlBatch","description":
  "Batch Extract-Transform-Load (ETL) Adapter","programType":"Workflow"}

  # Retrieving the available sink plugins of the ETL Batch Application Template
  $ curl -w'\n' -X GET 'http://localhost:10000/v3/templates/etlBatch/extensions/sink'

  [{"template":{"name":"etlBatch","description":"Batch Extract-Transform-Load (ETL)
  Adapter","programType":"Workflow"},"source":{"fileName":"cdap-etl-lib-3.0.0-SNAPSHOT.jar",
  "name":"cdap-etl-lib","version":{"version":"3.0.0-SNAPSHOT","major":3,"minor":0,"fix":0,"
  suffix":"SNAPSHOT"}},"type":"sink","name":"Stream","description":"Batch sink that outputs
  to the specified CDAP Stream"}]

  # Retrieving the information on the "Stream" sink plugin of the ETL Batch Application Template
  $ curl -w'\n' -X GET 'http://localhost:10000/v3/templates/etlBatch/extensions/sink/plugins/Stream'

  [{"className":"co.cask.cdap.templates.etl.realtime.sinks.StreamSink","properties":{"name":
  {"name":"name","description":"The name of the stream to output to. Must be a valid stream
  name. The stream will be created if it does not
  exist.","type":"string","required":true},"headers.field":{"name":"headers.field","
  description":"Name of the field in the record that contains headers. Headers are presumed
  to be a map of string to
  string.","type":"string","required":false},"body.field":{"name":"body.field","description"
  :"Name of the field in the record that contains the data to be written to the specified
  stream. The data could be in binary format as a byte array or a ByteBuffer. It can also be
  a String. If unspecified, the 'body' key is
  used.","type":"string","required":false}},"template":{"name":"etlBatch","description":"
  Batch Extract-Transform-Load (ETL)
  Adapter","programType":"Workflow"},"source":{"fileName":"cdap-etl-lib-3.0.0-SNAPSHOT.jar",
  "name":"cdap-etl-lib","version":{"version":"3.0.0-SNAPSHOT","major":3,"minor":0,"fix":0,"
  suffix":"SNAPSHOT"}},"type":"sink","name":"Stream","description":"Real-time sink that
  outputs to the specified CDAP Stream"}]


  # Realtime

  # Retrieving the available types of plugins of the ETL Realtime Application Template
  $ curl -w'\n' -X GET 'http://localhost:10000/v3/templates/etlRealtime'

  {"extensions":["sink","source","transform"],"name":"etlRealtime","description":"Realtime
  Extract-Transform-Load (ETL) Adapter","programType":"Worker"}

  # Retrieving the available sink plugins of the ETL Realtime Application Template
  $ curl -w'\n' -X GET 'http://localhost:10000/v3/templates/etlRealtime/extensions/sink'

  [{"template":{"name":"etlRealtime","description":"Realtime Extract-Transform-Load (ETL)
  Adapter","programType":"Worker"},"source":{"fileName":"cdap-etl-lib-3.0.0-SNAPSHOT.jar","
  name":"cdap-etl-lib","version":{"version":"3.0.0-SNAPSHOT","major":3,"minor":0,"fix":0,"
  suffix":"SNAPSHOT"}},"type":"sink","name":"Stream","description":"Real-time sink that
  outputs to the specified CDAP Stream"}]

  # Retrieving the information on the "Stream" sink plugin of the ETL Realtime Application Template
  $ curl -w'\n' -X GET 'http://localhost:10000/v3/templates/etlRealtime/extensions/sink/plugins/Stream'

  [{"className":"co.cask.cdap.templates.etl.realtime.sinks.StreamSink","properties":{"name":
  {"name":"name","description":"The name of the stream to output to. Must be a valid stream
  name. The stream will be created if it does not
  exist.","type":"string","required":true},"headers.field":{"name":"headers.field","
  description":"Name of the field in the record that contains headers. Headers are presumed
  to be a map of string to
  string.","type":"string","required":false},"body.field":{"name":"body.field","description"
  :"Name of the field in the record that contains the data to be written to the specified
  stream. The data could be in binary format as a byte array or a ByteBuffer. It can also be
  a String. If unspecified, the 'body' key is
  used.","type":"string","required":false}},"template":{"name":"etlRealtime","description":"
  Realtime Extract-Transform-Load (ETL)
  Adapter","programType":"Worker"},"source":{"fileName":"cdap-etl-lib-3.0.0-SNAPSHOT.jar","
  name":"cdap-etl-lib","version":{"version":"3.0.0-SNAPSHOT","major":3,"minor":0,"fix":0,"
  suffix":"SNAPSHOT"}},"type":"sink","name":"Stream","description":"Realtime sink that
  outputs to the specified CDAP Stream"}]


Creating an Adapter
=================================

Follow these steps to create an Adapter:

1. Using the API described above, determine which Application Template you would like to
   use, which Plugins from it you will use, and the required properties.

#. Complete a configuration JSON file ("configfile") following the format given above to 
   describe your Adapter.

#. Create the Adapter on a running instance of CDAP by using the JSON *configfile*::

    PUT <base-url>/namespaces/<namespace-id>/adapters/<adapter-name> -d "@/path/to/configfile"

  where:
  
  .. list-table::
     :widths: 20 80
     :header-rows: 1

     * - Parameter
       - Description
     * - ``<namespace-id>``
       - Namespace ID
     * - ``<adapter-name>``
       - Name of the Adapter







