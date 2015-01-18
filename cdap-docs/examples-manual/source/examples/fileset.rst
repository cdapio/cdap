.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform WordCount Application
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _examples-fileset:

=========
File Sets
=========

A Cask Data Application Platform (CDAP) Example demonstrating the FileSet dataset and its use in Services and MapReduce.

Overview
========

This application demonstrates the use of the FileSet dataset:

  - The ``lines`` FileSet is used as input for the ``WordCount`` MapReduce program.
  - The ``counts`` FileSet is used as output for the ``WordCount`` MapReduce program.
  - The ``FileSetService`` allows uploading and downloading files within these two file sets.

Let's look at some of these components, and then run the Application and see the results.

The FileSetExample Application
------------------------------

As in the other :ref:`examples,<examples-index>` the components
of the Application are tied together by the class ``FileSetExample``:

.. literalinclude:: /../../../cdap-examples/FileSetExample/src/main/java/co/cask/cdap/examples/fileset/FileSetExample.java
    :language: java
    :lines: 33-

The ``configure()`` method creates the two FileSet datasets used in this example. For the first
FileSet—``lines``—we specify an explicit base path; for the second FileSet—``counts``—we let
CDAP generate a path. Also, we configure the output format to use ":" as the separator for the
``counts`` FileSet.

We will use the ``FileSetService`` to upload a file into the ``lines`` file set, then count the words in
that file using MapReduce, and finally download the word counts from the ``counts`` file set.

FileSetService
--------------

This service has two handler methods: one to upload and another to download a file.
It declares the datasets that it needs to access using ``@UseDataSet`` annotations:

.. literalinclude:: /../../../cdap-examples/FileSetExample/src/main/java/co/cask/cdap/examples/fileset/FileSetService.java
    :language: java
    :lines: 58-62


Both methods of this Service have two arguments: the name of the FileSet and the relative path within that FileSet.
For example, the ``read`` method returns the contents of the requested file for a GET request:

.. literalinclude:: /../../../cdap-examples/FileSetExample/src/main/java/co/cask/cdap/examples/fileset/FileSetService.java
    :language: java
    :lines: 64-86

It uses the ``getLocation()`` of the file set to obtain the location representing the requested file,
and then opens an input stream for that location. ``Location`` is a file system abstraction from
`Apache™ Twill® <http://twill.incubator.apache.org>`__; you can read more about its interface in the Apache Twill
`Javadocs <http://twill.incubator.apache.org/apidocs/org/apache/twill/filesystem/Location.html>`__.

MapReduce over Files
====================

``WordCount`` is a simple word counting implementation in MapReduce. It reads its input from the
``lines`` file set and writes its output to the ``counts`` FileSet:

.. literalinclude:: /../../../cdap-examples/FileSetExample/src/main/java/co/cask/cdap/examples/fileset/WordCount.java
    :language: java
    :lines: 38-54

It is worth mentioning that nothing in ``WordCount`` is specifically programmed to use a FileSet. Instead of
``lines`` and ``counts``, it could use any other dataset as long as the key and value types match.

Building and Starting
=====================

- You can either build the example (as described `below
  <#building-an-example-application>`__) or use the pre-built JAR file included in the CDAP SDK.
- Start CDAP, deploy and start the application and its component as described below in 
  `Running CDAP Applications`_\ .
  Make sure you start the Service as described below.
- Once the application has been deployed and started, you can `run the example. <#running-the-example>`__

Running CDAP Applications
============================================

.. include:: /../../developers-manual/source/getting-started/building-apps.rst
    :start-line: 9

Running the Example
===================

Starting the Service
------------------------------

Once the application is deployed:

- Click on ``FileSetExample`` in the Overview page of the CDAP Console to get to the
  Application detail page, click ``FileSetService`` in the *Service* pane to get to the
  Service detail page, then click the *Start* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface:

  .. list-table::
    :widths: 20 80
    :stub-columns: 1

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh start service FileSetExample.FileSetService``
    * - On Windows:
      - ``> bin\cdap-cli.bat start service FileSetExample.FileSetService``    

Uploading and Downloading Files
-------------------------------
First we will upload a text file that we will use as input for the WordCount.
This is done by making a REST call to the ``FileSetService``.
A sample text file is included in the ``resources`` directory of the example::

  curl -v localhost:10000/v2/apps/FileSetExample/services/FileSetService/methods/lines?path=some.txt \
    -XPUT --data-binary @resources/lines.txt

Now we start the MapReduce program and configure it to use this file as its input, and to write its output to
``counts.out``::

  curl -v localhost:10000/v2/apps/FileSetExample/mapreduce/WordCount/start \
    -d '{ "dataset.lines.input.paths": "some.txt", \
          "dataset.counts.output.path": "counts.out" }'

Wait for the MapReduce program to finish, and you can download the results of the computation::

  curl -v localhost:10000/v2/apps/FileSetExample/services/FileSetService/methods/counts?path=counts.out/part-r-00000

Note that we have to download a part file that is under the output path that was specified for the MapReduce program.
This is because in MapReduce, every reducer writes a separate part file into the output directory.
In this case, as we have fixed the number of reducers to one, there is only a single part file to download.

**Note:** A version of ``curl`` that works with Windows is included in the CDAP Standalone
SDK in ``libexec\bin\curl.exe``

Stopping the Application
-------------------------------
Once done, you can stop the application as described above in `Stopping an Application. 
<#stopping-an-application>`__ Here is an example-specific description of the step:

**Stopping the Service**

- Click on ``FileSetExample`` in the Overview page of the CDAP Console to get to the
  Application detail page, click ``FileSetService`` in the *Service* pane to get to the
  Service detail page, then click the *Stop* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface:

  .. list-table::
    :widths: 20 80
    :stub-columns: 1

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh stop service FileSetExample.FileSetService``
    * - On Windows:
      - ``> bin\cdap-cli.bat stop service FileSetExample.FileSetService``    
