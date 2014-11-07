.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _streams:

============================================
Streams
============================================

**Streams** are the primary means of bringing data from external systems into the CDAP in realtime.
They are ordered, time-partitioned sequences of data, usable for realtime collection and consumption of data.

They can be created programmatically within your application, through the :ref:`CDAP
Console <cdap-console>`, or by using the :ref:`command-line tool. <cli>` Data written to a
Stream can be consumed in real-time by :ref:`Flows <flows>` or in batch by :ref:`MapReduce
Jobs. <mapreduce>`.

Streams are uniquely identified by an ID string (a "name") and are explicitly created
before being used. Names used for Streams need to be unique across the CDAP instance, as
Streams are shared between applications.

You specify a Stream in your :ref:`Application <applications>` specification::

  addStream(new Stream("myStream"));

This specifies a new Stream named *myStream*. 

You can write to Streams either one operation at a time or in batches, using either the
:ref:`http-restful-api-stream` or the :ref:`command-line tools. <cli>`

Each individual signal sent to a Stream is stored as a ``StreamEvent``, which is comprised
of a header (a map of strings for metadata) and a body (a blob of binary data).

To convert the binary data into a string, you need to take into account the character
encoding of the data, such as shown in this code fragment from a Flow::

  @ProcessInput
  public void process(StreamEvent myStreamEvent) {
    String event = Charsets.UTF_8.decode(myStreamEvent.getBody()).toString();
  ...
  }

Streams are included in just about every CDAP application, tutorial, guide or example; the
:ref:`Hello World <examples-hello-world>` demonstrates using a stream to ingest a name into 
a dataset.

For an example of pushing events to a Stream from the command-line, see the :ref:`Purchase
example <examples-purchase>`, and its script ``inject-data`` that injects data to a stream.

