.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2014-2016 Cask Data, Inc.

.. _http-restful-api-stream:

=======================
Stream HTTP RESTful API
=======================

.. highlight:: console

Use the CDAP Stream HTTP RESTful API to create a :ref:`stream <developers:streams>`; send,
read, and truncate events sent to and from a stream; and set the TTL property of a stream.

Streams may have multiple consumers (for example, multiple flows), each of which may be a
group of different agents (for example, multiple instances of a flowlet).

Additional details and examples are found in the :ref:`Developers' Manual: Streams <developers:streams>`.

.. Base URL explanation
.. --------------------
.. include:: base-url.txt


Creating a Stream
=================
A stream can be created with an HTTP PUT method to the URL::

  PUT /v3/namespaces/<namespace-id>/streams/<new-stream-id>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``new-stream-id``
     - Name of the stream to be created

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The event either successfully created a stream or the stream already exists

.. rubric:: Example
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``PUT /v3/namespaces/default/streams/mystream``
   * - Description
     - Create a new stream named *mystream* in the namespace *default* 

.. rubric:: Comments

- The ``new-stream-id`` should only contain ASCII letters, digits and hyphens.
- If the stream already exists, no error is returned, and the existing stream remains in place.

Optionally, properties for the stream can be set by providing them in the body of the request. These properties can
be retrieved and modified afterwards using the ``/properties`` endpoint.

.. list-table::
   :widths: 20 20 60
   :header-rows: 1

   * - Parameter
     - Default Value
     - Description
   * - ``ttl``
     - ``Long.MAX`` (2^63 - 1)
     - Number of seconds that an event will be valid for, since it was ingested
   * - ``format``
     - ``text``
     - JSON Object describing the format name, schema, and settings. Accepted formats are
       ``avro``, ``csv`` (comma-separated), ``tsv`` (tab-separated), ``text``, ``clf``,
       ``grok``, and ``syslog``.
   * - ``notification.threshold.mb``
     - 1024
     - Increment of data, in MB, that a stream has to receive before
       publishing a notification.
   * - ``description``
     - ``null``
     - Description of the stream

If a property is not given in the request body, the default value will be used instead.

Sending Events to a Stream
==========================
An event can be sent to a stream by sending an HTTP POST method to the URL of the stream::

  POST /v3/namespaces/<namespace-id>/streams/<stream-id>

In cases where it is acceptable to have some events lost, events can be transmitted
asynchronously to a stream with higher throughput by sending an HTTP POST method to the
``async`` URL::

  POST /v3/namespaces/<namespace-id>/streams/<stream-id>/async

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``stream-id``
     - Name of an existing stream

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The event was successfully received and persisted
   * - ``202 ACCEPTED``
     - The event was successfully received but may not be persisted; only the asynchronous endpoint will return this status code
   * - ``404 Not Found``
     - The stream does not exist


.. rubric:: Example
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``POST /v3/namespaces/default/streams/mystream``
   * - Description
     - Send an event to the existing stream named *mystream* in the namespace *default*

.. rubric:: Comments

You can pass headers for the event as HTTP headers by prefixing them with the *stream-id*::

  <stream-id>.<property>:<string value>

After receiving the request, the HTTP handler transforms it into a stream event:

- The body of the event is an identical copy of the bytes found in the body of the HTTP post request.
- If the request contains any headers prefixed with the *stream-id*,
  the *stream-id* prefix is stripped from the header name and
  the header is added to the event.

Sending Events to a Stream in Batch
===================================
Multiple events can be sent to a stream in batch by sending an HTTP POST method to the URL of the stream::

  POST /v3/namespaces/<namespace-id>/streams/<stream-id>/batch

The ``Content-Type`` header must be specified to describe the data type in the POST body. Currently, these
types are supported:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Content-Type
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``text/{sub-type}``
     - Text content with one line per event; the ``sub-type`` can be anything
   * - ``avro/binary``
     - Avro Object Container File format; each Avro record in the file becomes a single event in the stream

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - All events were successfully received and persisted
   * - ``404 Not Found``
     - The stream does not exist

.. rubric:: Example
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``POST /v3/namespaces/default/streams/mystream/batch``
   * - Content type header
     - ``Content-type: text/csv``
   * - POST body
     - A comma-separated record per line::
     
        1,Sam,Smith,18
        2,Max,Johnson,28
        3,Bill,Jones,20
        
   * - Description
     - Writes three comma-separated events to the stream named *mystream* in the namespace *default*

.. rubric:: Comments

You can pass headers that apply to all events as HTTP headers by prefixing them with the *stream-id*::

  <stream-id>.<property>:<string-value>

After receiving the request, if the request contains any headers prefixed with the *stream-id*,
the *stream-id* prefix is stripped from the header name and the header is added to each event sent
in the request body.

Reading Events from a Stream
============================
Reading events from an existing stream is performed with an HTTP GET method to the URL::

  GET /v3/namespaces/<namespace-id>/streams/<stream-id>/events?start=<startTime>&end=<endTime>&limit=<limit>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``stream-id``
     - Name of an existing stream
   * - ``startTime``
     - Optional timestamp in milliseconds to start reading events from (inclusive); default is 0
   * - ``endTime``
     - Optional timestamp in milliseconds for the last event to read (exclusive); default is the maximum timestamp (2^63)
   * - ``limit``
     - Optional maximum number of events to read; default is unlimited

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The event was successfully received and the result of the read was returned
   * - ``204 No Content``
     - The stream exists but there are no events that satisfy the request
   * - ``404 Not Found``
     - The stream does not exist

The response body is a JSON array with the stream event objects as array elements::

   [ 
     {"timestamp" : ... , "headers": { ... }, "body" : ... }, 
     {"timestamp" : ... , "headers": { ... }, "body" : ... } 
   ]

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Field
     - Description
   * - ``timestamp``
     - Timestamp in milliseconds of the stream event at ingestion time
   * - ``headers``
     - A JSON map of all custom headers associated with the stream event
   * - ``body``
     - A printable string representing the event body; non-printable bytes are hex escaped in the format ``\x[hex-digit][hex-digit]``, e.g. ``\x05``

.. rubric:: Example
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``GET /v3/namespaces/default/streams/mystream/events?limit=1``
   * - Description
     - Read the initial event from an existing stream named *mystream* in the namespace *default*
   * - Response body
     - ``[ {"timestamp" : 1407806944181, "headers" : { }, "body" : "Hello World" } ]``

Truncating a Stream
===================
Truncating means deleting all events that were ever written to the stream.
This is permanent and cannot be undone.
A stream can be truncated with an HTTP POST method to the URL::

  POST /v3/namespaces/<namespace-id>/streams/<stream-id>/truncate

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``stream-id``
     - Name of an existing stream

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The stream was successfully truncated
   * - ``404 Not Found``
     - The stream ``{stream-id}`` does not exist

.. rubric:: Example
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``POST /v3/namespaces/default/streams/mystream/truncate``
   * - Description
     - Delete all events in the stream named *mystream* in the namespace *default*

Deleting a Stream
=================
Deleting a stream means both deleting all events that were ever written to the stream and
the stream endpoint itself. This is permanent and cannot be undone. If another stream is
created with the same name, it will not return any of the previous stream's events.

A stream can be deleted with an HTTP DELETE method to the URL::

  DELETE /v3/namespaces/<namespace-id>/streams/<stream-id>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``stream-id``
     - Name of an existing stream

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The stream was successfully deleted
   * - ``404 Not Found``
     - The stream ``{stream-id}`` does not exist

.. rubric:: Example
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``DELETE /v3/namespaces/default/streams/mystream``
   * - Description
     - Deletes the stream named *mystream* in the namespace *default* and all events in
       the stream

.. _http-restful-api-stream-setting-properties:

Getting and Setting Stream Properties
=====================================
There are a number of stream properties that can be retrieved and specified.

The **Time-To-Live** (TTL, specified in seconds) property governs how long an event is valid for consumption since 
it was written to the stream.
The default TTL for all streams is infinite, meaning that events will never expire.

The **format** property defines how stream event bodies should be read for data exploration.
Different formats support different types of schemas. Schemas are used to determine
the table schema used for running ad-hoc SQL-like queries on the stream.
See :ref:`stream-exploration` for more information about formats and schemas.

The **notification threshold** defines the increment of data that a stream has to receive before
publishing a notification.

The **description** of the stream.

.. rubric:: Getting Stream Properties

Stream properties can be retrieved with an HTTP PUT method to the URL::

  GET /v3/namespaces/<namespace-id>/streams/<stream-id>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``stream-id``
     - Name of an existing stream

.. rubric:: Example
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``GET /v3/namespaces/default/streams/who``
       ::

         { 
           "ttl" : 9223372036854775,
           "format": {
             "name": "text",
             "schema": {
               "type": "record",
               "name": "stringBody",
               "fields": [
                 { "name": "body", "type": "string" }
               ]
             },
             "settings": {}
           },
           "notification.threshold.mb" : 1024,
           "description" : "Web access logs"
         }
     
   * - Description
     - Retrieves the properties of the ``who`` stream of the :ref:`HelloWorld example <examples-hello-world>`. 

.. rubric:: Setting Stream Properties

Stream properties can be changed with an HTTP PUT method to the URL::

  PUT /v3/namespaces/<namespace-id>/streams/<stream-id>/properties

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``stream-id``
     - Name of an existing stream

New properties are passed in the JSON request body.

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``ttl``
     - Number of seconds that an event will be valid for, since it was ingested
   * - ``format``
     - JSON Object describing the format name, schema, and settings. Accepted formats are
       ``avro``, ``csv`` (comma-separated), ``tsv`` (tab-separated), ``text``, ``clf``, 
       ``grok``, and ``syslog``.
   * - ``notification.threshold.mb``
     - Increment of data, in MB, that a stream has to receive before
       publishing a notification.
   * - ``description``
     - Description of the stream

If a property is not given in the request body, no change will be made to the value.
For example, setting format but not TTL will preserve the current value for TTL.
Changing the schema attached to a stream will drop the Hive table associated with
the stream and re-create it with the new schema.

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Stream properties were changed successfully
   * - ``400 Bad Request``
     - The TTL value is not a non-negative integer, the format was not known,
       the schema was malformed, or the schema is not supported by the format
   * - ``404 Not Found``
     - The stream does not exist

.. rubric:: Example
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``PUT /v3/namespaces/default/streams/mystream/properties``
       ::

         { 
           "ttl" : 86400,
           "format": {
             "name": "csv",
             "schema": {
               "type": "record",
               "name": "event",
               "fields": [
                 { "name": "f1", "type": "string" },
                 { "name": "f2", "type": "int" },
                 { "name": "f3", "type": "double" }
               ]
             },
             "settings": { "delimiter": " " }
           },
           "notification.threshold.mb" : 1000
         }
     
   * - Description
     - Change the TTL property of the stream named *mystream* in the namespace *default* to 1 day,
       and the format to CSV (comma-separated values) with a three field schema
       that uses a space delimiter instead of a comma delimiter. 

Streams used by an Application
==============================
You can retrieve a list of streams used by an application by issuing a HTTP GET request to the URL::

  GET /v3/namespaces/<namespace-id>/apps/<app-id>/streams

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``app-id``
     - Application ID

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Request was successful

Streams used by a Program
=========================
You can retrieve a list of streams used by a program by issuing a HTTP GET request to the URL::

  GET /v3/namespaces/<namespace-id>/apps/<app-id>/<program-type>/<program-id>/streams 

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``app-id``
     - Application ID
   * - ``program-type``
     - Program type, one of ``flows``, ``mapreduce``, ``services``, ``spark``, or ``workflows``
   * - ``program-id``
     - Program ID

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Request was successful

Programs using a Stream 
========================
You can retrieve a list of programs that are using a stream by issuing a HTTP GET request to the URL::

  GET /v3/namespaces/<namespace-id>/data/datasets/<dataset-id>/programs

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``dataset-id``
     - Dataset ID

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Request was successful
