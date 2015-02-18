.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _http-restful-api-stream:

===========================================================
Stream HTTP RESTful API
===========================================================

.. highlight:: console

This interface supports creation of a :ref:`Stream; <developers:streams>` sending, reading, and truncating events to
and from a Stream; and setting the TTL property of a Stream.

Streams may have multiple consumers (for example, multiple Flows), each of which may be a
group of different agents (for example, multiple instances of a Flowlet).


Creating a Stream
-----------------
A Stream can be created with an HTTP PUT method to the URL::

  PUT <base-url>/namespaces/<namespace-id>/streams/<new-stream-id>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace-id>``
     - Namespace ID
   * - ``<new-stream-id>``
     - Name of the Stream to be created

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The event either successfully created a Stream or the Stream already exists

.. rubric:: Example
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``PUT <base-url>/namespaces/default/streams/mystream``
   * - Description
     - Create a new Stream named *mystream* in the namespace *default* 

.. rubric:: Comments

- The ``<new-stream-id>`` should only contain ASCII letters, digits and hyphens.
- If the Stream already exists, no error is returned, and the existing Stream remains in place.

Sending Events to a Stream
--------------------------
An event can be sent to a Stream by sending an HTTP POST method to the URL of the Stream::

  POST <base-url>/namespaces/<namespace-id>/streams/<stream-id>

In cases where it is acceptable to have some events lost, events can be transmitted
asynchronously to a Stream with higher throughput by sending an HTTP POST method to the
``async`` URL::

  POST <base-url>/namespaces/<namespace-id>/streams/<stream-id>/async

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace-id>``
     - Namespace ID
   * - ``<stream-id>``
     - Name of an existing Stream

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
     - The Stream does not exist


.. rubric:: Example
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``POST <base-url>/namespaces/default/streams/mystream``
   * - Description
     - Send an event to the existing Stream named *mystream* in the namespace *default*

.. rubric:: Comments

You can pass headers for the event as HTTP headers by prefixing them with the *stream-id*::

  <stream-id>.<property>:<string value>

After receiving the request, the HTTP handler transforms it into a Stream event:

- The body of the event is an identical copy of the bytes found in the body of the HTTP post request.
- If the request contains any headers prefixed with the *stream-id*,
  the *stream-id* prefix is stripped from the header name and
  the header is added to the event.

Sending Events to a Stream in Batch
-----------------------------------
Multiple events can be sent to a Stream in batch by sending an HTTP POST method to the URL of the Stream::

  POST <base-url>/namespaces/<namespace-id>/streams/<stream-id>/batch

The ``Content-Type`` header must be specified to describe the data type in the POST body. Currently, these
types are supported:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Content-Type
     - Description
   * - ``<namespace-id>``
     - Namespace ID
   * - ``text/<sub-type>``
     - Text content with one line per event; the ``<sub-type>`` can be anything
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
     - The Stream does not exist

.. rubric:: Example
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``POST <base-url>/namespaces/default/streams/mystream/batch``
   * - Content type header
     - ``Content-type: text/csv``
   * - POST body
     - A comma-separated record per line::
     
        1,Sam,Smith,18
        2,Max,Johnson,28
        3,Bill,Jones,20
        
   * - Description
     - Writes three comma-separated events to the Stream named *mystream* in the namespace *default*

.. rubric:: Comments

You can pass headers that apply to all events as HTTP headers by prefixing them with the *stream-id*::

  <stream-id>.<property>:<string-value>

After receiving the request, if the request contains any headers prefixed with the *stream-id*,
the *stream-id* prefix is stripped from the header name and the header is added to each event sent
in the request body.

Reading Events from a Stream
----------------------------
Reading events from an existing Stream is performed with an HTTP GET method to the URL::

  GET <base-url>/namespaces/<namespace-id>/streams/<stream-id>/events?start=<startTime>&end=<endTime>&limit=<limit>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace-id>``
     - Namespace ID
   * - ``<stream-id>``
     - Name of an existing Stream
   * - ``<startTime>``
     - Optional timestamp in milliseconds to start reading events from (inclusive); default is 0
   * - ``<endTime>``
     - Optional timestamp in milliseconds for the last event to read (exclusive); default is the maximum timestamp (2^63)
   * - ``<limit>``
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
     - The Stream exists but there are no events that satisfy the request
   * - ``404 Not Found``
     - The Stream does not exist

The response body is a JSON array with the Stream event objects as array elements::

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
     - Timestamp in milliseconds of the Stream event at ingestion time
   * - ``headers``
     - A JSON map of all custom headers associated with the Stream event
   * - ``body``
     - A printable string representing the event body; non-printable bytes are hex escaped in the format ``\x[hex-digit][hex-digit]``, e.g. ``\x05``

.. rubric:: Example
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``GET <base-url>/namespaces/default/streams/mystream/events?limit=1``
   * - Description
     - Read the initial event from an existing Stream named *mystream* in the namespace *default*
   * - Response body
     - ``[ {"timestamp" : 1407806944181, "headers" : { }, "body" : "Hello World" } ]``

Truncating a Stream
-------------------
Truncating means deleting all events that were ever written to the Stream.
This is permanent and cannot be undone.
A Stream can be truncated with an HTTP POST method to the URL::

  POST <base-url>/namespaces/<namespace-id>/streams/<stream-id>/truncate

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace-id>``
     - Namespace ID
   * - ``<stream-id>``
     - Name of an existing Stream

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The Stream was successfully truncated
   * - ``404 Not Found``
     - The Stream ``<stream-id>`` does not exist

.. rubric:: Example
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``POST <base-url>/namespaces/default/streams/mystream/truncate``
   * - Description
     - Delete all events in the Stream named *mystream* in the namespace *default*

Setting Stream Properties
-------------------------
There are a number of Stream properties that can be specified.
The Time-To-Live (TTL) property governs how long an event is valid for consumption since 
it was written to the Stream.
The default TTL for all Streams is infinite, meaning that events will never expire.
The format property defines how Stream event bodies should be read for data exploration.
Different formats support different types of schemas. Schemas are used to determine
the table schema used for running ad-hoc SQL-like queries on the Stream.
See :ref:`stream-exploration` for more information about formats and schemas. 

Stream properties can be changed with an HTTP PUT method to the URL::

  PUT <base-url>/namespaces/<namespace-id>/streams/<stream-id>/config

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace-id>``
     - Namespace ID
   * - ``<stream-id>``
     - Name of an existing Stream

New properties are passed in the JSON request body.

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``ttl``
     - Number of seconds that an event will be valid for since ingested
   * - ``format``
     - JSON Object describing the format name, schema, and settings

If a property is not given in the request body, no change will be made to the value.
For example, setting format but not TTL will preserve the current value for TTL.
Changing the schema attached to a Stream will drop the Hive table associated with
the Stream and re-create it with the new schema.

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
     - The Stream does not exist

.. rubric:: Example
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``PUT <base-url>/namespaces/default/streams/mystream/config``::

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
           } 
         }
     
   * - Description
     - Change the TTL property of the Stream named *mystream* in the namespace *default* to 1 day,
       and the format to CSV (comma separated values) with a three field schema
       that uses a space delimiter instead of a comma delimiter. 
