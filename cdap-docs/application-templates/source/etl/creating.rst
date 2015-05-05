.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _apptemplates-etl-creating:

==============================
Creating An ETL Adapter 
==============================

.. highlight:: console

Introduction
============
Adapters can be created using the :ref:`RESTful API <http-restful-api-apptemplates-adapters-creating>`,
the :ref:`CDAP CLI <cli>`, or the :ref:`CDAP UI <cdap-ui>`. In order to create an Adapter,
an Adapter configuration is required that specifies the source, transformations and sink
along with their properties.  (In the CDAP UI, the interface prompts you for the required
information.)


.. _apptemplates-etl-configuration-file-format:

Configuration File Format
=========================

Using the ETL Batch Template
----------------------------

With the ETL Batch Template, it requires a ``schedule`` property with a cron entry
specifying the frequency of the Batch job run, such as every day or every hour.

For example, this JSON (when in a file such as ``config.json``) provides a
configuration for a Batch Adapter that runs every minute, reading data from a Stream
*myStream* and writing to a Dataset (Table) called *myTable*,  without any transformations::

  {
    "template":"ETLBatch",
    "description":"Batch ETL",
    "config":{
        "schedule":"* * * * *",
        "source":{
            "name":"Stream",
            "properties":{  
                "name":"myStream",
                "duration":"1m"
            }
        },
        "transforms":[],
        "sink":{
            "name":"Table",
            "properties":{
                "name":"myTable",
                "schema.row.field":"ts",
            }
        }
     }
  }

The Adapter launches a MapReduce program that runs every minute, reads data from the
Stream *myStream* and writes to a Table *myTable*. A Table Sink needs a row key field to
be specified and can use the timestamp of a Stream event for that.

To create an instance of this Adapter, called *streamAdapter*:

- Using the :ref:`RESTful API <http-restful-api-apptemplates-adapters-creating>`::

    PUT /v3/namespaces/default/adapters/streamAdapter -d @config.json 

- Using :ref:`CDAP CLI <cli>`::

    $ create adapter streamAdapter <path-to-config.json>

where ``config.json`` is the file that contains the Adapter configuration.


.. highlight:: console

Using the ETL Realtime Template
-------------------------------

This next configuration creates a Realtime Adapter that reads from Twitter and writes to a
Stream after performing a projection transformation::

  {
    "template":"ETLRealtime",
    "description":"Twitter to Stream: renames the 'message' field name to 'tweet',
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
                    "drop":"lang,time,favCount,source,geoLat,geoLong,isRetweet"
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


The ETL Realtime Template expects an instance property that will create *N* instances of the
adapter that run concurrently. In Standalone CDAP mode, this is implemented as multiple threads;
while in Distributed CDAP mode, it will create different YARN containers. The
number of instances of a Realtime Adapter cannot be changed during runtime.

The ``instances`` property value needs to be greater than 0. Note that the ``instance``
property replaces the ``schedule`` property of the ETL Batch Template.

In this case, we will use a *ProjectionTransform* (a type of Transform) to drop certain
columns in the incoming data. A *StreamSink* in the final step needs a data field property
that it will use as the content for the data to be written. 
