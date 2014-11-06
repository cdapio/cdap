.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _streams:

============================================
Streams
============================================

**Streams** are the primary means of bringing data from external systems into the CDAP in realtime.
They are ordered, time-partitioned sequences of data, usable for realtime collection and consumption of data.



You specify a Stream in your :ref:`Application <applications>` specification::

  addStream(new Stream("myStream"));

This specifies a new Stream named *myStream*. 

Streams are uniquely identified by an ID string (a "name") and are explicitly created
before being used. Names used for Streams need to be unique across the CDAP instance, as
Streams are shared between applications.

They can be created programmatically within your application, through the CDAP Console, or
by using a command-line tool. Data written to a Stream can be consumed in real-time by
:ref:`Flows <flows>` or in batch by :ref:`MapReduce Jobs. <mapreduce>`.
You can write to Streams either one operation at a time or in batches,
using either the :ref:`http-restful-api-stream`
or command-line tools.

Each individual signal sent to a Stream is stored as a ``StreamEvent``, which is comprised
of a header (a map of strings for metadata) and a body (a blob of binary data).


