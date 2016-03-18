.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform WordCount Application
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _examples-fileset:

===============
FileSet Example
===============

A Cask Data Application Platform (CDAP) example demonstrating the FileSet dataset and its
use in services and MapReduce.


Overview
========
This application demonstrates the use of the FileSet dataset:

- The *lines* FileSet is used as input for the *WordCount* MapReduce program.
- The *counts* FileSet is used as output for the *WordCount* MapReduce program.
- The *FileSetService* allows uploading and downloading files within these two file sets.

Let's look at some of these components, and then run the application and see the results.

The FileSetExample Application
------------------------------
As in the other :ref:`examples <examples-index>`, the components
of the application are tied together by the class ``FileSetExample``:

.. literalinclude:: /../../../cdap-examples/FileSetExample/src/main/java/co/cask/cdap/examples/fileset/FileSetExample.java
    :language: java
    :lines: 33-

The ``configure()`` method creates the two FileSet datasets used in this example. For the first
FileSet |---| *lines* |---| we specify an explicit base path; for the second FileSet
|---| *counts* |---| we let CDAP generate a path. Also, we configure the output format
to use ``":"`` as the separator for the *counts* FileSet.

We will use the *FileSetService* to upload a file into the *lines* file set, then count the words in
that file using MapReduce, and finally download the word counts from the *counts* file set.

FileSetService
--------------
This service has two handler methods: one to upload and another to download a file.
Both methods of this service have two arguments: the name of the FileSet and the relative path within that FileSet.
For example, the ``read`` method returns the contents of the requested file for a GET request:

.. literalinclude:: /../../../cdap-examples/FileSetExample/src/main/java/co/cask/cdap/examples/fileset/FileSetService.java
    :language: java
    :lines: 62-84
    :dedent: 4

It first instantiates the dataset specified by the first path parameter through its ``HttpServiceContext``.
Note that, conceptually, this method is not limited to using only the two datasets of this application (*lines* and
*counts*) |---| ``getDataset()`` can dynamically instantiate any existing dataset.

The handler method then uses the ``getLocation()`` of the FileSet to obtain the location representing the requested
file, and it opens an input stream for that location. ``Location`` is a file system abstraction from
`Apache™ Twill® <http://twill.incubator.apache.org>`__; you can read more about its interface in the
`Apache Twill Javadocs <http://twill.incubator.apache.org/apidocs/org/apache/twill/filesystem/Location.html>`__.

Note that after obtaining the location from the FileSet, the handler discards that dataset through its context |---|
it is not needed any more and therefore can be returned to the system. This is not strictly necessary: all datasets
are eventually reclaimed by the system. However, explicitly discarding a dataset allows the system to reclaim it
as soon as the current transaction ends, possibly freeing valuable resources.

The ``write`` method uses an ``HttpContentConsumer`` to stream the body of the request to the location specified
by the ``path`` query parameter. See the section on :ref:`Handling Large Requests <services-content-consumer>`
and the :ref:`Sport Results Example <examples-sport-results>` for a more detailed explanation:

.. literalinclude:: /../../../cdap-examples/FileSetExample/src/main/java/co/cask/cdap/examples/fileset/FileSetService.java
     :language: java
     :lines: 86-135
     :dedent: 4

MapReduce over Files
====================
``WordCount`` is a simple word counting implementation in MapReduce. It reads its input from the
*lines* FileSet and writes its output to the *counts* FileSet:

.. literalinclude:: /../../../cdap-examples/FileSetExample/src/main/java/co/cask/cdap/examples/fileset/WordCount.java
    :language: java
    :lines: 40-57
    :append: ...

It is worth mentioning that nothing in ``WordCount`` is specifically programmed to use a FileSet. Instead of
*lines* and *counts*, it could use any other dataset as long as the key and value types match.


.. Building and Starting
.. =====================
.. |example| replace:: FileSetExample
.. |example-italic| replace:: *FileSetExample*
.. |application-overview-page| replace:: :cdap-ui-apps-programs:`application overview page, programs tab <FileSetExample>`
.. include:: _includes/_building-starting-running.txt


Running the Example
===================

.. Starting the Service
.. --------------------
.. |example-service| replace:: FileSetService
.. |example-service-italic| replace:: *FileSetService*

.. include:: _includes/_starting-service.txt

Uploading and Downloading Files
-------------------------------
First, we will upload a text file (``some.txt``) that we will use as input for the WordCount.
This is done by making a RESTful call to the *FileSetService*.
A sample text file (``lines.txt``) is included in the ``resources`` directory of the example::

  $ cdap-cli.sh call service FileSetExample.FileSetService PUT "lines?path=some.txt" body:file examples/FileSetExample/resources/lines.txt

Now, we start the MapReduce program and configure it to use the file ``some.txt`` as its input, and to write its output to
``counts.out``::

  $ cdap-cli.sh start mapreduce FileSetExample.WordCount \"dataset.lines.input.paths=some.txt dataset.counts.output.path=counts.out\"
  Successfully started MapReduce program 'WordCount' of application 'FileSetExample' with provided runtime arguments 'dataset.lines.input.paths=some.txt dataset.counts.output.path=counts.out'

Check the status of the MapReduce program until it is completed::

  $ cdap-cli.sh get mapreduce status FileSetExample.WordCount
  STOPPED


and you can download the results of the computation::

  $ cdap-cli.sh call service FileSetExample.FileSetService GET "counts?path=counts.out/part-r-00000"
  < 200 OK
  < Content-Length: 60
  a:1
  five:1
  hello:4
  is:1
  letter:1
  say:1
  the:1
  word:2
  world:1

Note that we have to download a part file that is under the output path that was specified for the MapReduce program.
This is because in MapReduce, every reducer writes a separate part file into the output directory.
In this case, as we have fixed the number of reducers to one, there is only a single part file to download.


.. Stopping and Removing the Application
.. =====================================
.. include:: _includes/_stopping-service-removing-application.txt
