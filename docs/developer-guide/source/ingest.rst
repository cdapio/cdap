.. :author: Cask Data, Inc.
   :description: Introduction to the Cask Data Application Platform
   :copyright: Copyright © 2014 Cask Data, Inc.

======================================================
Ingesting Data into the Cask Data Application Platform
======================================================

Introduction
============

One of the first tasks of actually working with Big Data applications is getting the data in.
To assist in the ingestion of data into Cask Data Application Platform (CDAP) Applications, we have
assembled a set of tools and applications that covers the major use cases:

- Java, Python and Ruby APIs for controlling and writing to Streams; 
- a drop zone for bulk ingestion;
- a daemon process that tails local files; and
- an Apache Flume Sink implementation for writing events received from a source.


Tools
=====

Stream Client
--------------------

The Stream Client is for managing Streams via external applications. It is available in three different
APIs: Java, Python and Ruby. 

Supported Actions
.................

 - Create a Stream with a specified *stream-id*;
 - Update the TTL (time-to-live) for an existing Stream with a specified *stream-id*;
 - Retrieve the current Stream TTL for a specified *stream-id*;
 - Truncate an existing Stream (the deletion of all events that were written to the Stream);
 - Write an event to an existing Stream; and
 - Send a File to an existing Stream.
 
Example (using Java API)
........................
   
Create a StreamClient instance, specifying the fields 'host' and 'port' of the gateway server. 
Optional configurations that can be set:
  
- SSL: true or false (use HTTP protocol) 
- WriterPoolSize: '10' (max thread pool size for write events to the Stream)
- Version : 'v2' (Gateway server version, used as a part of the base URI 
  ``http(s)://localhost:10000/v2/...``) 
- AuthToken: null (Need to specify to authenticate client requests) 
- APIKey: null (Need to specify to authenticate client requests using SSL)
 
::

  StreamClient streamClient = new RestStreamClient.Builder("localhost", 10000).build();

or specified using the builder parameters::

  StreamClient streamClient = new RestStreamClient.Builder("localhost", 10000)
                                                  .apiKey("apiKey")
                                                  .authToken("token")
                                                  .ssl(false)
                                                  .version("v2")
                                                  .writerPoolSize(10)
                                                  .build();

 
Create a new Stream with the *stream id* "newStreamName"::

  streamClient.create("newStreamName");
      
**Notes:**
 
- The *stream-id* should only contain ASCII letters, digits and hyphens.
- If the Stream already exists, no error is returned, and the existing Stream remains in place.
 
Update TTL for the Stream *streamName*; TTL is a long value::

  streamClient.setTTL("streamName", newTTL);

Get the current TTL value for the Stream *streamName*::

  long ttl = streamClient.getTTL("streamName");  

Create a ``StreamWriter`` instance for writing events to the Stream *streamName*::

   StreamWriter streamWriter = streamClient.createWriter("streamName");

To write new events to the Stream, you can use any of these five methods in the ``StreamWriter`` interface::

  ListenableFuture<Void> write(String str, Charset charset);
  ListenableFuture<Void> write(String str, Charset charset, Map<String, String> headers);
  ListenableFuture<Void> write(ByteBuffer buffer);
  ListenableFuture<Void> write(ByteBuffer buffer, Map<String, String> headers);
  ListenableFuture<Void> send(File file, MediaType type);

Example::

  streamWriter.write("New log event", Charsets.UTF_8).get();

To truncate the Stream *streamName*, use::

  streamClient.truncate("streamName");
 
When you are finished, release all resources by calling these two methods::

  streamWriter.close();
  streamClient.close();


Java API
--------------------

Available at: [link]


Python API
--------------------

Available at: [link]


Ruby API
--------------------

Available at: [link]


File Drop Zone
--------------------

The File DropZone application allows you to easily perform the bulk ingestion of local files.

Features
........

- Distributed as debian and rpm packages;
- Loads properties from configuration file;
- Supports multiple observers/topics;
- Able to survive restart and resume, sending from the first unsent record of each of the existing files; and
- Cleanup of files that are completely sent.

Available at: [link]

File Tailer
--------------------

File Tailer is a daemon process that performs tailing of sets of local files. 
As soon as a new record has been appended to the end of a file that the daemon is monitoring, 
it will send it to a Stream via the REST API.

Features
........

- Distributed as debian and rpm packages;
- Loads properties from a configuration file;
- Supports rotation of log files;
- Persists state and is able to resume from first unsent record; and
- Writes statistics info.

Available at: [link]


Flume Sink
--------------------

The CDAP Sink is a `Apache Flume Sink <https://flume.apache.org>`__ implementation using the
RESTStreamWriter to write events received from a source. For example, you can configure the Flume Sink's
Agent to read data from a log file by tailing it and putting them into CDAP.

Available at: [link]

Where to Go Next
================
Now that you've looked at tools for ingesting data into CDAP, take a look at:

- `Querying Datasets with SQL <query.html>`__,
  which covers ad-hoc querying of CDAP Datasets using SQL.


.. |(TM)| unicode:: U+2122 .. trademark sign
   :trim: