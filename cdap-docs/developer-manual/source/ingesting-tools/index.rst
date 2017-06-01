.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _ingesting-data:

================================================
Ingesting Data into CDAP
================================================

.. toctree::
   :maxdepth: 1
   
   cdap-stream-clients-java
   cdap-stream-clients-python
   cdap-stream-clients-ruby
   cdap-flume
..   cdap-stream-clients-javascript

.. rubric:: Introduction

One of the first tasks of actually working with Big Data applications is getting the data in.
As data ingestion is a fundamental issue, and as one tool often does not fit all needs,
we have assembled tools and applications to assist with ingesting data into CDAP:

- Java, Python, and Ruby APIs for controlling and writing to streams; and
- an Apache Flume Sink implementation for writing events received from a source.
