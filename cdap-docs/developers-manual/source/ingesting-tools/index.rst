.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _ingesting-data:

================================================
Ingesting Data into CDAP
================================================

.. toctree::
   :maxdepth: 1
   
   cdap-stream-clients-java
   cdap-stream-clients-javascript
   cdap-stream-clients-python
   cdap-stream-clients-ruby
   cdap-flume
   cdap-file-drop-zone
   cdap-file-tailer

.. rubric:: Introduction

One of the first tasks of actually working with Big Data applications is getting the data in.
As data ingestion is a fundamental issue, and as one tool often does not fit all needs,
we have assembled tools and applications to assist with ingesting data into CDAP:

- Java, JavaScript, Python, and Ruby APIs for controlling and writing to Streams;
- an Apache Flume Sink implementation for writing events received from a source;
- a drop zone for bulk ingestion of files; and
- a File Tailer daemon to tail local files.

**Note:** *The File DropZone and File Tailer are no longer supported for CDAP 3.0. Most likely,
they will still work, but because File DropZone and File Tailer are not aware of CDAP
Namespaces, they would only work with the default namespace.*
