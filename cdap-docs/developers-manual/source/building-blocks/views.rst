.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _views:

=====
Views
=====

*Views* are a read-only source from where data can be read. They are similar to a
:ref:`stream <streams>` or :ref:`dataset <datasets-index>`.

A view has a specific read format. Read formats consist of a :ref:`schema
<stream-exploration-stream-schema>` and a :ref:`format <stream-exploration-stream-format>`
(such as CSV, TSV, or Avro, amongst others).

If :ref:`CDAP Explore <data-exploration>` is :ref:`enabled
<configuration-explore-service>`, a Hive table will be created for each view that is
created.

Currently, views are only supported for streams. Support for datasets will be added in a
later version of CDAP.

Views can be created, deleted, listed, and their details retrieved using the 
:ref:`Views HTTP RESTful API <http-restful-api-views>` and using the 
:ref:`CDAP CLI <cli>`, and in particular with the CLI's 
:ref:`Ingest Commands <cli-available-commands>`. 

Creating and Modifying a Stream View
====================================
A view can be added to an existing stream with either an 
:ref:`HTTP POST request command <http-restful-api-view-creating-stream-view>` or a 
matching CDAP CLI command. In the request body is a JSON object specifying the :ref:`read
format <stream-exploration-stream-format>` to be used::

  PUT <base-url>/namespaces/<namespace>/streams/<stream-id>/views/<view-id>
  
If a stream view for that stream already exists, it will be modified instead of created.
Only the response code will differ.

Deleting a Stream View
========================
To delete an existing stream view, issue an :ref:`HTTP DELETE request
<http-restful-api-view-deleting-stream-view>` (or the corresponding CLI command)::

  DELETE <base-url>/namespaces/<namespace>/streams/<stream-id>/views/<view-id>

Listing Views and View Details
==============================
To list all of the existing stream views of an existing stream, issue an HTTP GET request
to the URL::

  GET <base-url>/namespaces/<namespace>/streams/<stream-id>/views

For detailed information on an existing stream view, issue the same command, but passing in the
ID of the view you are interested in::

  GET <base-url>/namespaces/<namespace>/streams/<stream-id>/views/<view-id>

Further details can be found in the :ref:`Views HTTP RESTful API <http-restful-api-views>`.