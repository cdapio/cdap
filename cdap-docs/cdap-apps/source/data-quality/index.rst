.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _cdap-apps-data-quality-index:

========================
Data Quality Application
========================

Overview
========
The goal of the **Data Quality Application** is to provide users with an extensible CDAP application to help them
determine the quality of their data.  Users can assess the quality of their data using its out-of-the-box
functionality and libraries. The application can be extended with custom aggregation functions and queried with a
RESTful API to obtain the results of the quality metric computations.

In this guide, you'll learn to use the *Data Quality Application*, including:

- deploying the application with the default functionality;
- configuring the application to your specifications;
- building custom aggregation functions; and
- querying the aggregated data.

Creating the Application
------------------------
The application can be created from the ``cdap-data-quality`` system artifact by supplying an application configuration:

.. container:: highlight

  .. parsed-literal::
    {
      "artifact": {
        "name": "cdap-data-quality",
        "version": "|version|",
        "scope": "system"
      },
      "config": {
        "source": {
          "name": "Stream",
          "id": "logStream",
          "properties": {
            "duration": "5m",
            "format": "clf",
            "name": "logStream"
          }
        },
        "datasetName": "dataQuality",
        "workflowScheduleMinutes": 5,
        "fieldAggregations": {
          "remote_host": [ "DiscreteValuesHistogram" ],
          "remote_login": [ "DiscreteValuesHistogram" ],
          "auth_user": [ "DiscreteValuesHistogram" ],
          "date": [ "DiscreteValuesHistogram" ],
          "request": [ "DiscreteValuesHistogram" ],
          "status": [ "DiscreteValuesHistogram" ],
          "content_length": [ "DiscreteValuesHistogram" ],
          "referrer": [ "DiscreteValuesHistogram" ],
          "user_agent": [ "DiscreteValuesHistogram" ]
        }
      }
    }


* ``source``: Data Quality Source

  - ``name``: Name of the :ref:`Batch Source Plugin <cdap-apps-etl-plugins-batchsources>`.
  - ``id``: Unique ID that can be used to query for Data Quality metrics using DataQualityService
  - ``properties``: Properties required by the :ref:`Batch Source Plugin <cdap-apps-etl-plugins-batchsources>`
  
* ``workflowScheduleMinutes``: Frequency (in minutes) with which the workflow runs the aggregation MapReduce
* ``datasetName``: Name of the destination dataset
* ``fieldAggregations``: Map that relates each field value to a set of aggregation functions

Deploying the Application
-------------------------
To deploy the application with the application configuration, issue a PUT curl call.
In this example, the ``appconfig.json`` file contains the application configuration::

  $ curl -w'\n' localhost:10000/v3/namespaces/default/apps/StreamDQ -d @appconfig.json -X PUT -H 'Content-Type: application/json'

Note that the ``cdap-data-quality`` artifact uses the same batch source plugins as the ``cdap-etl-batch`` artifact.
See :ref:`Batch Sources <cdap-apps-etl-plugins-batchsources>` for more information about available plugins. 

End-to-End Example
==================

Let's take the example of a user who wants to use the Data Quality Application to:

- ingest a stream of CLF log data;
- generate histograms partitioned by the time distribution of status codes;
- generate these aggregations every 10 minutes; and
- query ranges of timestamps for aggregated histogram data.

We would create a Data Quality Application by creating a JSON file ``appconfig.json`` that contains:

.. container:: highlight

  .. parsed-literal::
    {
      "artifact": {
        "name": "cdap-data-quality",
        "version": "|version|",
        "scope": "system"
      },
      "config": {
        "workflowScheduleMinutes": 5,
        "source": {
          "name": "Stream",
          "id": "logStream",
          "properties": {
            "duration": "5m",
            "format": "clf",
            "name": "logStream"
          }
        },
        "datasetName": "dataQuality",
        "inputFormat": "clf",
        "fieldAggregations": {
          "status": [ "DiscreteValuesHistogram" ]
        }
      }
    }

To deploy the application, issue this curl command::

  $ curl -w'\n' localhost:10000/v3/namespaces/default/apps/StreamDQ -d @appconfig.json -X PUT -H 'Content-Type: application/json'

Next, resume the workflow schedule::
 
  $ curl -w'\n' -X POST localhost:10000/v3/namespaces/default/apps/StreamDQ/schedules/aggregatorSchedule/resume 

This will resume the schedule so that it kicks off a workflow run every five minutes.
Now, let's send some data to the stream. We can do this by using the RESTful API::

  $ curl -w'\n' localhost:10000/v3/namespaces/default/streams/logStream \
  -d '93.184.216.34 - - [08/Feb/2015:04:54:14 +0000] "GET /examples/example1 HTTP/1.0" 200 1343488 "http:/example.com/" "Mozilla/5.0 (Windows NT 6.1; rv:33.0) Gecko/20100101 Firefox/33.0"'
  
  $ curl -w'\n' localhost:10000/v3/namespaces/default/streams/logStream \
  -d '93.184.216.34 - - [08/Feb/2015:04:54:14 +0000] "GET /examples/example2 HTTP/1.0" 404 34234 "http:/example.com/" "Mozilla/5.0 (Windows NT 6.1; rv:33.0) Gecko/20100101 Firefox/33.0"'
  
  $ curl -w'\n' localhost:10000/v3/namespaces/default/streams/logStream \
  -d '93.184.216.34 - - [08/Feb/2015:04:54:14 +0000] "GET /examples/example3 HTTP/1.0" 400 88234 "http:/example.com/" "Mozilla/5.0 (Windows NT 6.1; rv:33.0) Gecko/20100101 Firefox/33.0"'

Once the MapReduce has completed, we can then query the aggregated data.
In order to make queries, we need to start the service in the application::

  $ curl -w'\n' -X POST localhost:10000/v3/namespaces/default/apps/StreamDQ/services/DataQualityService/start

There are four RESTful endpoints of the service which we can use:

* Retrieve all the fields for a given source and time range: ``/v1/sources/{sourceID}/fields``
* Retrieve all the aggregations available for a given source, time range, and field: 
  ``/v1/sources/{sourceID}/fields/{fieldName}/aggregations``
* Retrieve the combined aggregation over a specified time interval for a given source, field, and aggregation type: 
  ``/v1/sources/{sourceID}/fields/{fieldName}/aggregations/{aggregationType}/totals``
* Retrieve the aggregation corresponding to each time stamp in a specified time range for a given source, field, and 
  aggregation type: ``/v1/sources/{sourceID}/fields/{fieldName}/aggregations/{aggregationType}/timeseries``

Suppose we want to query the aggregated data for the source ``logStream`` and the field ``status``. 
We would make this request::

  $ curl -w'\n' http://localhost:10000/v3/namespaces/default/apps/StreamDQ/services/DataQualityService/methods/v1/sources/logStream/fields/status/aggregations/DiscreteValuesHistogram/totals

If you use the above sample Apache Access logs, your response should look like this: 

.. code:: json

  {
     "200":1,
     "400":1,
     "404":1
  }

Built-in Library of Aggregation Functions
=========================================
* Discrete Values Histogram: Generates a histogram that maps various values of a field to corresponding frequencies. 
  This is only for discrete data.  
* Histogram With Bucketing: Generates a histogram that maps various values of a field to corresponding frequencies. 
  This is only for numerical data. 
* Unique Values: Generates the number of unique values seen for a given field. 
* Mean: Generates the mean of all the values seen for a given field. This is only for numerical data.
* Standard Deviation: Generates the standard deviation of all the values seen for a given field. 
  This is only for numerical data. 

Extending the Application
=========================
In addition to the built-in aggregation functions with the Data Quality Application, it's
possible to write and use custom aggregation functions. Take a look at the
``BasicAggregationFunction.java`` and ``CombinableAggregationFunction.java`` interfaces
under the ``functions`` directory (``DQApp/src/main/java/data/quality/app/functions``).

All aggregation functions need to implement the ``BasicAggregationFunction`` interface. If
a function produces aggregations that can be combined (for example, frequencies can be
combined while standard deviations cannot), it should also implement the
``CombinableAggregationFunction`` interface. This will let you combine existing
aggregations over custom time ranges at query time.

In addition to the these interfaces, take a look at the aggregation functions that come
with the application for reference.
