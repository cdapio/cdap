.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

.. _streams:

=======
Streams
=======

*Streams* are the primary means of bringing data from external systems into the CDAP in real time.
They are ordered, time-partitioned sequences of data, usable for real-time collection and consumption of data.

They can be created programmatically within your application, using the
:ref:`http-restful-api-stream`, the :ref:`stream-client` of the :ref:`client-api`, or by
using the :ref:`CDAP Command Line Interface. <cli>` 

Data written to a stream can be consumed in real time by :ref:`flows <flows>` or in batch
by :ref:`MapReduce programs. <mapreduce>`.

.. include:: /security/index.rst
    :start-after: .. impersonation-start
    :end-before: .. impersonation-end


Creating a Stream
=================
You specify a stream in your :ref:`application <applications>` specification::

  addStream(new Stream("myStream"));

This specifies a new stream named *myStream*. 

Streams are uniquely identified by a combination of the :ref:`namespace <namespaces>` and
the stream name and are explicitly created before being used. Names used for streams need
to be unique within a :ref:`namespace <namespaces>`, as streams are shared between applications.
Names that start with an underscore (``_``) will not be visible in the home page of the
:ref:`CDAP UI <cdap-ui>`, though they will be visible elsewhere in the CDAP UI.


Writing To a Stream
===================
You can write to streams either one operation at a time or in batches, using either the
:ref:`http-restful-api-stream` or the :ref:`Command Line Interface. <cli>`

Each individual signal sent to a stream is stored as a ``StreamEvent``, which is comprised
of a header (a map of strings for metadata) and a body (a blob of binary data).


Reading From a Stream
=====================
To convert the binary data into a string, you need to take into account the character
encoding of the data, such as shown in this code fragment::

  @ProcessInput
  public void process(StreamEvent myStreamEvent) {
    String event = Charsets.UTF_8.decode(myStreamEvent.getBody()).toString();
  ...
  }


Stream Time-To-Live (TTL)
=========================
Streams are persisted by CDAP, and once an event has been sent to a stream, by default it
never expires. The Time-To-Live (TTL, specified in seconds) property governs how long an event is valid for
consumption since it was written to the stream. The default TTL for all streams is
infinite, meaning that events will never expire. The TTL property of a stream can be
changed, using the :ref:`http-restful-api-stream`, the :ref:`stream-client` of the
:ref:`client-api`, or by using the :ref:`Command Line Interface. <cli>`


Truncating and Deleting a Stream
================================
Streams can be truncated, which means deleting all events that were ever written to the
stream. This is permanent and cannot be undone. They can be truncated using the
:ref:`http-restful-api-stream`, the :ref:`stream-client` of the :ref:`client-api`, or
by using the :ref:`Command Line Interface <cli>`.

Deleting a stream means deleting the endpoint so that events can no longer be written to
it. This is permanent and cannot be undone. If another stream is created with the same
name, it will not return any of the previous stream's events.


Stream Examples
===============
Streams are included in just about every CDAP :ref:`application <apps-and-packs>`,
:ref:`tutorial <tutorials>`, :ref:`guide <guides-index>` or :ref:`example <examples-index>`.

- The simplest example, :ref:`Hello World <examples-hello-world>`, demonstrates **using a
  stream to ingest** a name into a dataset.

- For an example of **pushing events to a stream,** see the :ref:`Purchase
  example <examples-purchase>` and its CDAP CLI command that injects data to a stream.

- For an example of **reading events from a stream,** see the 
  :ref:`Purchase example <examples-purchase>`, where the class ``PurchaseStreamReader``
  reads events from a stream. 

- For an example of **reading from a stream with a MapReduce program,** see the 
  :ref:`cdap-mapreduce-guide`, where the class ``TopClientsMapReduce``
  reads events from a stream using the method ``Input.ofStream()``. 
