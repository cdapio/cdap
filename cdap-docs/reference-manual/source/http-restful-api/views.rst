.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _http-restful-api-views:

======================
Views HTTP RESTful API 
======================

.. highlight:: console

Views are a read-only source from where data can be read. They are similar to a
:ref:`stream <streams>` or :ref:`dataset <datasets-index>`.

A view has a specific read format. Read formats consist of a :ref:`format <stream-exploration-stream-format>`
(such as CSV, TSV, or Avro, amongst others) and a :ref:`schema <stream-exploration-stream-schema>`.
Some formats (:ref:`CSV, TSV <stream-exploration-stream-format_csv_tsv>`) use an additional *settings* 
attribute to complete the mapping of data to fields.

:ref:`CDAP Explore <data-exploration>` needs to be :ref:`enabled <hadoop-configuration-explore-service>` 
before any views are created so that a Hive table can be created for each view. (See the
:ref:`CDAP installation instructions <installation-index>` for your particular Hadoop
distribution for details.)

Currently, views are only supported for streams. Support for datasets will be added in a
later version of CDAP.


.. _http-restful-api-view-creating-stream-view:

Creating a Stream View
======================
A view can be added to an existing stream with an HTTP POST request to the URL::

  PUT <base-url>/namespaces/<namespace>/streams/<stream-id>/views/<view-id>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<stream-id>``
     - Name of the stream (must be already existing)
   * - ``<view-id>``
     - Name of the view to be created, or, if already existing, updated

The request body is a JSON object specifying the :ref:`read format <stream-exploration-stream-format>` to be used. 

An existing stream view can be modified with the same command; only the response code will differ.

.. rubric:: Example

For example, to create a stream view *logStreamView* on an existing stream
*logStream*, using the format *clf* and a schema, you could use (reformatted for display)::
 
  $ curl -w'\n' -X PUT 'http://localhost:10000/v3/namespaces/default/streams/logStream/views/logStreamView' \
    -H "Content-Type: application/json" -d \
    '{
      "format": {
        "name": "clf",
        "schema": 
          "{
            \"type\":\"record\",
            \"name\":\"event\",
            \"fields\":[
              {\"name\":\"remotehost\",\"type\":\"string\"},
              {\"name\":\"rfc931\",\"type\":\"string\"},
              {\"name\":\"authuser\",\"type\":\"string\"},
              {\"name\":\"date\",\"type\":\"date\"},
              {\"name\":\"request\",\"type\":\"string\"},
              {\"name\":\"status\",\"type\":\"string\"},
              {\"name\":\"bytes\",\"type\":\"int\"}     
          ]}",
        "settings": {
          "key1":"val1",
          "key2":"val2"
        }
      }
    }'

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - An existing stream view was successfully modified
   * - ``201 CREATED``
     - A new stream view was created


.. _http-restful-api-view-listing-stream-view:

Listing Existing Stream Views
=============================
To list all of the existing stream views of an existing stream, issue an HTTP GET request
to the URL::

  GET <base-url>/namespaces/<namespace>/streams/<stream-id>/views

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<stream-id>``
     - Name of the stream (must be already existing)

The response body is a JSON object with a list of all the views currently existing for the
specified stream. 

.. rubric:: Example

For example, to see all the stream views on an existing stream *logStream*, you could use::
 
  $ curl -w'\n' -X GET 'http://localhost:10000/v3/namespaces/default/streams/logStream/views'
    
    ["logStreamView1", "logStreamView2", ...]

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - A list of stream views for the specified stream was successfully retrieved
   * - ``404 Not Found``
     - The specified stream was not found


.. _http-restful-api-view-details-stream-view:

Details of a Stream View
========================
For detailed information on an existing stream view, issue an HTTP GET request
to the URL::

  GET <base-url>/namespaces/<namespace>/streams/<stream-id>/views/<view-id>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<stream-id>``
     - Name of the stream
   * - ``<view-id>``
     - Name of the existing view
     
.. rubric:: Example

For example, to see the details of the stream view *logStreamView* on the stream
*logStream*, you could use (pretty-printed for display)::
 
  $ curl -w'\n' -X GET 'http://localhost:10000/v3/namespaces/default/streams/logStream/view/logStreamView'
    
  {"id":"logStreamView",
   "format":
      {"name":"clf",
       "schema":
          {"type":"record",
           "name":"event",
           "fields":[{"name":"remotehost","type":["string","not-null"]},...],
           "settings":{...}
          } 
      },
   "tableName":"stream_logStream_logStreamView"
  }

     
.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - A JSON object describing the specified stream view was successfully retrieved
   * - ``404 Not Found``
     - Either the specified stream view or the specified stream was not found
     

.. _http-restful-api-view-deleting-stream-view:

Deleting a Stream View
========================
To delete an existing stream view, issue an HTTP DELETE request to the URL::

  DELETE <base-url>/namespaces/<namespace>/streams/<stream-id>/views/<view-id>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<stream-id>``
     - Name of the stream
   * - ``<view-id>``
     - Name of the existing view
     
.. rubric:: Example

For example, to delete the stream view *logStreamView* on the stream
*logStream*, you could use::
 
  $ curl -w'\n' -X DELETE 'http://localhost:10000/v3/namespaces/default/streams/logStream/view/logStreamView'
     
.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The specified stream view was successfully deleted
   * - ``404 Not Found``
     - Either the specified stream view or the specified stream was not found
