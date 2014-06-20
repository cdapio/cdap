================================================
Introduction to the Continuuity Reactor REST API
================================================

.. reST Editor: .. section-numbering::
.. reST Editor: .. contents::

.. rst2pdf: CutStart
.. landslide: theme ../_theme/slides-generation/
.. landslide: build ../../html/

.. include:: ../_slide-fragments/continuuity_logo_copyright.rst

.. |br| raw:: html

   <br />
.. rst2pdf: CutStop

.. rst2pdf: config ../../../developer-guide/source/_templates/pdf-config
.. rst2pdf: stylesheets ../../../developer-guide/source/_templates/pdf-stylesheet
.. rst2pdf: build ../../pdf/
.. rst2pdf: .. |br|  unicode:: U+0020 .. space

----

Module Objectives
=================

In this module, you will look at the basic elements of 
Continuuity Reactor through its REST API:

- REST API Conventions
- Streams
- Flows
- DataSets
- Procedures
- Reactor Applications and Lifecycle Management
- Logs
- Metrics

----

Continuuity REST API
====================

The Continuuity Reactor has an HTTP interface for:

- **Streams:** sending data events to a Stream, or to inspect the contents of a Stream
- **Flows:** query and setting number of instances
- **DataSets:** interacting with DataSets (currently limited to Tables)
- **Procedures:** sending queries to a Procedure
- **Reactor:** deploying and managing Applications
- **Logs:** retrieving Application logs
- **Metrics:** retrieving metrics for system and user Applications (user-defined metrics)

Note: The HTTP interface binds to port ``10000``

----

Continuuity Reactor REST API Conventions
========================================

All URLs referenced in the API have this base URL::

	http://<gateway>:10000/v2

where ``<gateway>`` is the URL of the Continuuity Reactor. The base URL is represented as::

	<base-url>

For example::

	PUT <base-url>/streams/<new-stream-id>

means
::

	PUT http://<gateway>:10000/v2/streams/<new-stream-id>

----

Streams and Flows
=================

Stream API supports:

- Creating Streams
- Sending events to a Stream
- Reading single events from a Stream
- Streams may have multiple consumers (e.g., multiple Flows),
  each of which may be a group of different agents
  (e.g., multiple Flowlet instances)
- To read events from a Stream, client application must first obtain a consumer
  (group) id, which is passed to subsequent read requests

Flows API supports: 

- Query and set the number of instances executing a given Flowlet

----

DataSets
========

The Data API allows you to interact with Continuuity Reactor Tables
(the core DataSets) through HTTP:

- Create Tables
- Read data
- Write data
- Modify data
- Delete data

Note: Deleting or dropping tables is not possible through this API,
though you can truncate the DataSet using this API

----

Procedures
==========

The REST API supports sending queries to the methods of an Application’s procedures:

- Send the method name as part of the request URL
- Send the arguments as a JSON string in the body of the request
- Used to retrieve results from the Reactor

----

Reactor Applications and Lifecycle Management
=============================================

Use the Reactor Client HTTP API to:

- Deploy or delete Applications
- Manage the life cycle of Flows, Procedures and MapReduce Jobs

----

Logs
====

You can download the logs that are emitted by any of the 
elements running in the Continuuity Reactor

Logs are emitted by:

- Flows
- Procedures
- MapReduce Jobs
- WorkFlows

----

Metrics
=======

As Applications process data, the Continuuity Reactor collects metrics
about the Application’s behavior and performance

Some Metrics are the same for every Application and are called **System** or **Reactor** metrics:

- How many events are processed
- How many data operations are performed
- etc.

----

Metrics: User-defined
=====================

Other metrics are **User-defined** and differ from Application to Application:

- Embed user-defined metrics in the methods defining the elements of your application
- They will then emit their metrics
- Retrieve them using the Continuuity Reactor’s REST interfaces

----

Module Summary
==============

You should now be familiar with:

- Reactor REST API Convention
- Streams and Flows
- DataSets
- Procedures
- Reactor Applications and Lifecycle Management
- Logs
- Metrics

----

Module Completed
================

`Chapter Index <return.html#m06>`__
