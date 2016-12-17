.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform WordCount Application
    :copyright: Copyright © 2014-2016 Cask Data, Inc.

.. _examples-fileset:

===============
FileSet Example
===============

A Cask Data Application Platform (CDAP) example demonstrating the FileSet dataset and its
use in services and MapReduce. The application also shows how to perform dataset management
operations in programs.

Overview
========
This application demonstrates the use of the FileSet dataset:

- The *lines* FileSet is used as input for the *WordCount* MapReduce program.
- The *counts* FileSet is used as output for the *WordCount* MapReduce program.
- The *FileSetService* allows uploading and downloading files within these two file sets. It also allows creating
  and managing additional file sets that can be used as input or output.

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
This service has one method to upload and another to download a file.
Both of these methods have two arguments: the name of the FileSet and the relative path within that FileSet.
For example, the ``read`` method returns the contents of the requested file for a GET request:

.. literalinclude:: /../../../cdap-examples/FileSetExample/src/main/java/co/cask/cdap/examples/fileset/FileSetService.java
    :language: java
    :lines: 79-102
    :dedent: 4

It first instantiates the dataset specified by the first path parameter through its ``HttpServiceContext``.
Note that, conceptually, this method is not limited to using only the two datasets of this application (*lines* and
*counts*) |---| ``getDataset()`` can dynamically instantiate any existing dataset.

The handler method then uses the ``getLocation()`` of the FileSet to obtain the location representing the requested
file, and it opens an input stream for that location. ``Location`` is a file system abstraction from
`Apache™ Twill® <http://twill.apache.org>`__; you can read more about its interface in the
`Apache Twill Javadocs <http://twill.apache.org/apidocs/org/apache/twill/filesystem/Location.html>`__.

Note that after obtaining the location from the FileSet, the handler discards that dataset through its context |---|
it is not needed any more and therefore can be returned to the system. This is not strictly necessary: all datasets
are eventually reclaimed by the system. However, explicitly discarding a dataset allows
the system to reclaim it either immediately (as in this case) or as soon as the current
transaction ends; in any case, possibly freeing valuable resources.

The ``write`` method uses an ``HttpContentConsumer`` to stream the body of the request to the location specified
by the ``path`` query parameter. See the section on :ref:`Handling Large Requests <services-content-consumer>`
and the :ref:`Sport Results Example <examples-sport-results>` for a more detailed explanation:

.. literalinclude:: /../../../cdap-examples/FileSetExample/src/main/java/co/cask/cdap/examples/fileset/FileSetService.java
    :language: java
    :lines: 110-159
    :dedent: 4

In addition to reading and writing individual files, the ``FileSetService`` also allows performing dataset management
operations, including creating, updating, truncating, and dropping file sets. These operations are available through
the program context's ``getAdmin()`` interface. For example, the service has an endpoint that creates a new file set,
either by cloning an existing file set's dataset properties, or using the properties submitted in the request body:

.. literalinclude:: /../../../cdap-examples/FileSetExample/src/main/java/co/cask/cdap/examples/fileset/FileSetService.java
    :language: java
    :lines: 171-201
    :dedent: 4

MapReduce over Files
====================
``WordCount`` is a simple word counting implementation in MapReduce. By default, it reads its input from the
*lines* FileSet and writes its output to the *counts* FileSet. Alternatively, the names of the input and output
dataset can also be given as runtime arguments:

.. literalinclude:: /../../../cdap-examples/FileSetExample/src/main/java/co/cask/cdap/examples/fileset/WordCount.java
    :language: java
    :lines: 33-65
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
A sample text file (``lines.txt``) is included in the ``resources`` directory of the example. From within the CDAP CLI:

.. tabbed-parsed-literal::
    :tabs: "CDAP CLI"

    |cdap >| call service FileSetExample.FileSetService PUT lines?path=some.txt body:file examples/FileSetExample/resources/lines.txt

    < 200 OK
    < Content-Length: 0
    < Connection: keep-alive
    < Content-Type: text/plain

Now, we start the MapReduce program and configure it to use the file ``some.txt`` as its input, and to write its output to
``counts.out``:

.. tabbed-parsed-literal::
    :tabs: "CDAP CLI"
    
    |cdap >| start mapreduce FileSetExample.WordCount "dataset.lines.input.paths=some.txt dataset.counts.output.path=counts.out"
  
    Successfully started MapReduce program 'WordCount' of application 'FileSetExample' 
    with provided runtime arguments 'dataset.lines.input.paths=some.txt dataset.counts.output.path=counts.out'

Check the status of the MapReduce program until it is completed:

.. tabbed-parsed-literal::
    :tabs: "CDAP CLI"
    
    |cdap >| get mapreduce status FileSetExample.WordCount
  
    STOPPED

and you can download the results of the computation:

.. tabbed-parsed-literal::
    :tabs: "CDAP CLI"
    
    |cdap >| call service FileSetExample.FileSetService GET counts?path=counts.out/part-r-00000
  
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

Dataset Management Operations
-----------------------------

Instead of using the default input and output datasets that are created when the application is deployed,
we can also create additional file sets using the ``FileSetService``. For example, we can create an input
dataset named ``myin`` using the ``create`` endpoint:

.. tabbed-parsed-literal::
    :tabs: "CDAP CLI"
    
    |cdap >| call service FileSetExample.FileSetService POST /myin/create body "{ 'properties': { 'input.format' : 'org.apache.hadoop.mapreduce.lib.input.TextInputFormat' } }"

The dataset properties for the new file set are given in the body of the request. Alternatively, we can clone an
existing dataset's properties to create an output file set named ``myout``:

.. tabbed-parsed-literal::
    :tabs: "CDAP CLI"
    
    |cdap >| call service FileSetExample.FileSetService POST /myout/create?clone=counts

We can now run the example with these two datasets as input and output. First, upload a file to the input dataset:

.. tabbed-parsed-literal::
    :tabs: "CDAP CLI"
    
    |cdap >| call service FileSetExample.FileSetService PUT myin?path=some.txt body:file examples/FileSetExample/resources/lines.txt

Now start the MapReduce and provide extra runtime arguments to specify the input and output dataset:

.. tabbed-parsed-literal::
    :tabs: "CDAP CLI"
    
    |cdap >| start mapreduce FileSetExample.WordCount "input=myin output=myout dataset.myin.input.paths=some.txt dataset.myout.output.path=counts.out"

Then we can retrieve the output of the MapReduce as previously:

.. tabbed-parsed-literal::
    :tabs: "CDAP CLI"
    
    |cdap >| call service FileSetExample.FileSetService GET myout?path=counts.out/part-r-00000

Finally, we can delete the two datasets that we created:

.. tabbed-parsed-literal::
    :tabs: "CDAP CLI"
    
    |cdap >| call service FileSetExample.FileSetService POST /myin/drop
    |cdap >| call service FileSetExample.FileSetService POST /myout/drop

.. Stopping and Removing the Application
.. =====================================
.. include:: _includes/_stopping-service-removing-application.txt
