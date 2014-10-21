.. :author: Cask Data, Inc.
   :copyright: Copyright © 2014 Cask Data, Inc.

.. _ingesting-data:

================================================
Ingesting Data into CDAP
================================================

.. highlight:: console

Introduction
============

One of the first tasks of actually working with Big Data applications is getting the data in.
As data ingestion is a fundamental issue, and as one tool often does not fit all needs,
we have assembled a set of tools and applications to assist in ingesting data into CDAP:

- Java and Python APIs for controlling and writing to Streams;
- a drop zone for bulk ingestion of files ;
- a File Tailer daemon to tail local files; and
- an Apache Flume Sink implementation for writing events received from a source.


.. highlight:: console

.. include:: ../_includes/cdap-stream-clients-java.rst
   
.. highlight:: console

.. include:: ../_includes/cdap-stream-clients-python.rst

.. highlight:: console

.. include:: ../_includes/cdap-file-drop-zone.rst

.. highlight:: console

.. include:: ../_includes/cdap-file-tailer.rst

.. highlight:: console

.. include:: ../_includes/cdap-flume.rst
