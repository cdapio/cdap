.. :author: Cask Data, Inc.
   :description: placeholder
   :copyright: Copyright © 2014 Cask Data, Inc.

.. _streams:

============================================
Stream
============================================

**Streams** are the primary means of bringing data from external systems into the CDAP in realtime.
They are ordered, time-partitioned sequences of data, usable for realtime collection and consumption of data.
You specify a Stream in your :ref:`Application <applications>` specification::

  addStream(new Stream("myStream"));

specifies a new Stream named *myStream*. Names used for Streams need to
be unique across the CDAP instance.

You can write to Streams either one operation at a time or in batches,
using either the :ref:`Cask Data Application Platform HTTP RESTful API <rest-streams>`
or command line tools.

Each individual signal sent to a Stream is stored as a ``StreamEvent``,
which is comprised of a header (a map of strings for metadata) and a
body (a blob of arbitrary binary data).

Streams are uniquely identified by an ID string (a "name") and are
explicitly created before being used. They can be created
programmatically within your application, through the CDAP Console,
or by or using a command line tool. Data written to a Stream
can be consumed in real-time by Flows or in batch by MapReduce. Streams are shared
between applications, so they require a unique name.
