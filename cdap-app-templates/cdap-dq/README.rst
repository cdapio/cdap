============================
Data Quality Application
============================

The goal of the Data Quality Application is to provide users with an extensible CDAP application to help them
determine the quality of their data.  Users could assess the quality of their data using its out-of-the-box
functionality and libraries. The application can be extended with custom aggregation functions and queried with a
RESTful API to obtain the results of the quality metric computations.

In this guide, you'll learn to use the Data Quality Application, including:

- deploying the application with the default functionality;
- configuring the application to your specifications;
- building custom aggregation functions; and
- querying the aggregated data.


What You Will Need
==================

- `JDK 7 or 8 <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`__
- `Apache Maven 3.1+ <http://maven.apache.org/>`__
- `CDAP SDK <http://docs.cdap.io/cdap/current/en/developers-manual/getting-started/standalone/index.html>`__


Building and Starting
=====================

The Data Quality application can be built and packaged using the Apache Maven command from the project root::

  $ mvn clean package

If you haven't already started a standalone CDAP installation, start it with the command::

  $ cdap.sh start

Deploying Application
---------------------
The application can be created from the Data Quality Artifact by supplying an application configuration:

.. code:: json

  {
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
        "referrer": [
            "DiscreteValuesHistogram"
        ],
        "status": [
            "DiscreteValuesHistogram"
        ],
        "remote_login": [
            "DiscreteValuesHistogram"
        ],
        "request": [
            "DiscreteValuesHistogram"
        ],
        "user_agent": [
            "DiscreteValuesHistogram"
        ],
        "auth_user": [
            "DiscreteValuesHistogram"
        ],
        "content_length": [
            "DiscreteValuesHistogram"
        ],
        "date": [
            "DiscreteValuesHistogram"
        ],
        "remote_host": [
            "DiscreteValuesHistogram"
        ]
    }
  }

* ``source`` : Data Quality Source
  - ``name``: Name of the Batch Source Plugin
  - ``id``: Unique Id that can be used to query for Data Quality metrics using DataQualityService
  - ``properties``: Properties required by the Batch Source Plugin
* ``workflowScheduleMinutes`` : Frequency (in minutes) with which the workflow runs the aggregation MapReduce.
* ``datasetName`` : Name of the destination dataset.
* ``fieldAggregations`` : Map that relates each field value to a set of aggregation functions.


To deploy the application with the application configuration, issue a PUT curl call.
In this example, the ``appconfig.json`` file contains the application configuration::

  $ curl -v localhost:10000/v3/namespaces/default/apps/StreamDQ -d @appconfig.json -X PUT -H 'Content-Type: application/json'

End-to-End Example
==================

Let's take the example of a user who wants wants to use the Data Quality Application to::

- Ingest a stream of CLF log data.
- Generate several histograms partitioned by time of distributions of status codes.
- Generate the aforementioned aggregations every 10 minutes.
- Query ranges of timestamps for aggregated histogram data.


We would create a Data Quality Application by creating a JSON file ``appconfig.json`` that contains:

.. code:: json

  {
      "workflowScheduleMinutes": 10,
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
          "status": [
              "DiscreteValuesHistogram"
          ]
      }
  }

To deploy the application, issue this curl command::

  $ curl -v localhost:10000/v3/namespaces/default/apps/StreamDQ -d @appconfig.json -X PUT -H 'Content-Type: application/json'

Now, let's send some data to the stream. We can do this by going to the UI (http://localhost:9999), clicking on
"logStream" -> "Actions" -> "Send Event". Enter each of the following Apache Access Log strings in the dialog box, and hit "Send Event" (one-by-one)::

  93.184.216.34 - - [08/Feb/2015:04:54:14 +0000] "GET /examples/example1 HTTP/1.0" 200 1343488 "http:/example.com/" "Mozilla/5.0 (Windows NT 6.1; rv:33.0) Gecko/20100101 Firefox/33.0"
  
  93.184.216.34 - - [08/Feb/2015:04:54:14 +0000] "GET /examples/example2 HTTP/1.0" 404 34234 "http:/example.com/" "Mozilla/5.0 (Windows NT 6.1; rv:33.0) Gecko/20100101 Firefox/33.0"
  
  93.184.216.34 - - [08/Feb/2015:04:54:14 +0000] "GET /examples/example3 HTTP/1.0" 400 88234 "http:/example.com/" "Mozilla/5.0 (Windows NT 6.1; rv:33.0) Gecko/20100101 Firefox/33.0"


Once we've done that, we can go back to the home page and wait for the workflow to start the MapReduce. Once the
MapReduce has completed, we can then query the aggregated data.

There are four RESTful endpoints which we can use:

* Retrieve all the fields for a given source and time range: ``/v1/sources/{sourceID}/fields``
* Retrieve all the aggregations available for a given source, time range, and field : ``/v1/sources/{sourceID}/fields/{fieldName}/aggregations``
* Retrieve the combined aggregation over a specified time interval for a given source, field, and aggregation type: ``/v1/sources/{sourceID}/fields/{fieldName}/aggregations/{aggregationType}/totals``
* Retrieve the aggregation corresponding to each time stamp in a specified time range for a given source, field, and aggregation type: ``/v1/sources/{sourceID}/fields/{fieldName}/aggregations/{aggregationType}/timeseries``

Suppose we want to be able to query the aggregated data for the source ``logStream`` and the field ``status``. 

We would make this request::

  $ curl -w'\n' http://localhost:10000/v3/namespaces/default/apps/DataQualityApp/services/AggregationsService/methods/v1/sources/logStream/fields/status/aggregations/DiscreteValuesHistogram/totals

If you use the aforementioned sample Apache Access logs, your response should look like this: 

.. code:: json

  {
     "200":1,
     "400":1,
     "404":1
  }

Built-in Library of Aggregation Functions
=========================================
* Discrete Values Histogram: Generates a histogram that maps various values of a field to corresponding frequencies. This is only for discrete data.  
* Histogram with Bucketing: Generates a histogram that maps various values of a field to corresponding frequencies. This is only for numerical data. 
* Unique: Generates the number of unique values seen for a given field. 
* Mean: Generates the mean of all the values seen for a given field. This is only for numerical data.
* Standard Deviation: Generates the standard deviation of all the values seen for a given field. This is only for numerical data. 

Extending the Application
=========================
In addition to the built-in aggregation functions with the Data Quality Application, 
it's possible to write and use custom aggregation functions. Take a look at the ``BasicAggregationFunction.java``
and ``CombinableAggregationFunction.java`` interfaces under the ``functions`` directory (``DQApp/src/main/java/data/quality/app/functions``).

All aggregation functions will need to implement the ``BasicAggregationFunction`` interface. If the function produces aggregations that can be combined (for example, frequencies can be combined but standard
deviations cannot), it should also implement the ``CombinableAggregationFunction`` interface if you want to be able to combine existing aggregations over custom time ranges at query time.

In addition to the aforementioned interfaces, take a look at the aggregation functions that come with the application for reference.

Share and Discuss!
==================

Have a question? Discuss at the `CDAP User Mailing List <https://groups.google.com/forum/#!forum/cdap-user>`__.

License
=======

Copyright © 2015 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License. You may obtain
a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
