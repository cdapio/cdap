.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _users-etl-creating:

=======================
Creating An ETL Adaptor
=======================

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

.. highlight:: console

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

.. highlight:: console

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


Example ETL Adaptors
====================

Example Batch Adaptor
---------------------

In this example, we look at creating an Adapter of type ETL Batch that reads from a
CDAP Stream and writes to a CDAP Table and that runs every 30 mins. This will launch a
MapReduce program that runs every 30 minutes and reads data from the Stream *myStream* and
writes to a Table *myTable*. A Table Sink needs a row key field to be specified and can use the
timestamp of a Stream event for that.

[John: how do you find out that it’s "schema.row.field" : "ts"?] 

.. highlight:: console

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

Example Realtime Adaptor
---------------------

In this example, we look at creating an Adapter of type ETL Realtime that receives data from
Twitter, performs processing on the data, and then writes it to a CDAP Stream *twitterStream*.

In this case, we will use a *ProjectionTransform* (a type of Transform) to rename a field in the incoming data
from "message" to "tweet". Stream Sink needs a data field property that it will use as the
content for the data to be written. The instances property is set to 1 and thus it will
launch a thread or a yarn container (depending on the runtime). The number of instances
of a Realtime Adapter cannot be changed during runtime.
 

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
                        "name":"Table",
                        "properties":{
                                       "name":"myTable",
                                       "schema.row.field":"ts"
                                     }
                      }
             }
  }

Configuring ETL Adaptors and Plugins
====================================

In order to configure an ETL Adapter, you’ll need information about the different ETL
plugins you are using: which sources, transforms, and sinks are available, and what
properties need to be specified for each of them.

This information can be retrieved from the platform using the CDAP ETL Adaptor HTTP
RESTful API. You can access this with the CDAP CLI (Command Line Interface), ``curl``
calls on a command line, or [through the CDAP UI].

In each of the following endpoints, ``<template-id>`` refers to either ``etlBatch`` or
``etlRealtime``. For example, if you fetch the information on sources for the
``etlRealtime`` template, it will provide information about all the realtime sources that
are available in the platform. 



/v3/templates/{template-id}/extensions/{type}/plugins

template-id : etlBatch or etlRealtime
type : source, transform, sink (or other plugins that you might have added [link])

Example and Sample response : ?
/v3/templates/{template-id}/extensions/{type}/plugins/{plugin-id}


template-id : etlBatch or etlRealtime
type : source, transform, sink (or other plugins that you might have added [link])
plugin-id : name of the plugin


Examples and Sample responses : ?

These endpoints are specific to ETL Plugins. For endpoints that are applicable to all
plugins [link] (including the ones you might have created) are documented here [link].







Old Notes
=========
(please disregard for now)

With UI
=======
- Start with an App-Template
  - ETL-Batch
  - ETL-Realtime
- Set Source
- Set Sink
- Set Transformation(s), if any

With CLI
========
??

With RESTful API
================
??

- With Java?
- With Java Client?
- With Other Clients: Javascript, Python, Ruby?
