.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform WordCount Application
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _examples-user-profiles:

=============
User Profiles
=============

A Cask Data Application Platform (CDAP) Example demonstrating column-level conflict
detection in Datasets using the example of managing user profiles in a Table.

Overview
========

This application demonstrates the use of the column-level conflict detection in a Dataset.
The fields of a user profile are updated in different ways:

  - Attributes such as name and email address are changed through a RESTful call when the 
    user updates their profile.
  - The time of the last login is updated by a sign-on service every
    time the user logs in, also through a RESTful call.
  - The time of the last activity is updated by a flow that processes
    events whenever it encounters an event from that user.

Let's look at some of these components, and then run the Application and see the results.

Introducing a Feature: Column-Level Conflict Detection
======================================================

As in the other :ref:`examples,<examples-index>` the components
of the Application are tied together by the class ``UserProfiles``:

.. literalinclude:: /../../../cdap-examples/UserProfiles/src/main/java/co/cask/cdap/examples/profiles/UserProfiles.java
    :language: java
    :lines: 32-

This application uses a table with conflict detection at the column-level.

A conflict occurs if two transactions that overlap in time modify the same data in a table.
For example, a flowlet's process method might overlap with a service handler.
Such a conflict is detected at the time that the transactions are committed,
and the transaction that attempts to commit last is rolled back.

By default, the granularity of the conflict detection is at the row-level.
That means it is sufficient for two overlapping transactions writing to
the same row of a table to cause a conflict, even if they write to different
columns.

Specifying a conflict detection level of ``COLUMN`` means that a conflict is
only detected if both transactions modify the same column of the same row.
This is more precise, but it requires more book-keeping in the transaction
system and can thus impact performance.



UserProfileService
--------------


Building and Starting
=====================

To observe conflict detection at both the row-level and column-level, you will need to modify 
and build this example twice. The first time, you will use row-level conflict detection, and see
errors appearing in a log. The second time, you will use column-level conflict detection and
see the scripts complete successfully without errors.
  
Build the Application with Row-level Conflict Detection
-------------------------------------------------------

Before building the application, set the ``ConflictDetection`` appropriately in the class ``UserProfiles``:

.. literalinclude:: /../../../cdap-examples/UserProfiles/src/main/java/co/cask/cdap/examples/profiles/UserProfiles.java
      :language: java
      :lines: 43-45
      
- The first time you build the application, set the ``tableProperties`` to ``ConflictDetection.ROW``. 

- Build the example (as described `below <#building-an-example-application>`__).
- Start CDAP, deploy and start the application and its component as described below in 
  `Running CDAP Applications`_\ .
  Make sure you start the Flow and Service as described below.
- Once the application has been deployed and started, you can `run the example. <#running-the-example>`__
- You should observe errors as described.

- Now, rebuild the application, setting the ``tableProperties`` back to its original value, ``ConflictDetection.COLUMN``.
- Re-build the application.
- Delete the  ``profiles`` Dataset on the 


Running CDAP Applications
============================================

.. include:: /../../developers-manual/source/getting-started/building-apps.rst
    :start-line: 9

Running the Example
===================

Delete any Existing Datasets
-----------------------------

Starting the Service and the Flow
---------------------------------

Once the application is deployed:

- Click on ``UserProfiles`` in the Overview page of the CDAP Console to get to the
  Application detail page, click:
  
  - ``FileSetService`` in the *Service* pane to get to the Service detail page, then click the *Start* button; and
  - ``ActivityFlow`` in the *Flow* pane to get to the Flow detail page, then click the *Start* button; or
  
- From the Standalone CDAP SDK directory, use the Command Line Interface:

  .. list-table::
    :widths: 20 80
    :stub-columns: 1

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh start service UserProfiles.FileSetService``
        ``$ ./bin/cdap-cli.sh start flow UserProfiles.ActivityFlow``

    * - On Windows:
      - ``> bin\cdap-cli.bat start service UserProfiles.FileSetService``    
        ``> bin\cdap-cli.bat start flow UserProfiles.ActivityFlow``    

Populate the ``profiles`` Table
-------------------------------

Populate the ``profiles`` tables with users using a script. From the example's directory, use:

  .. list-table::
    :widths: 20 80
    :stub-columns: 1

    * - On Linux:
      - ``$ ./bin/add-users.sh``

    * - On Windows:
      - ``> bin\add-users.bat``    


Create a Conflict
-----------------
Now, from two
different terminals, run the following concurrently:

- ``bin/update-login.sh`` to randomly update the time of last login for users; and
- ``bin/send-events.sh`` to generate random user activity events and send them to the stream.

If both scripts are running at the same time, then some user profiles will be updated at
the same time by the service and by the flow. With row-level conflict detection, you would
see transaction conflicts in the logs. But because the ``profiles`` table uses column
level conflict detection, these conflicts are avoided.




If you are interested to see the behavior with row-level conflict detection, you can change
the dataset creation statement at the bottom of``UserProfiles.java`` to use ``ConflictDetection.ROW``
and run the same steps as above. You should see transaction conflicts in the logs. For example, such
a conflict would shows as:

```
2015-01-26 21:00:20,084 - ERROR [FlowletProcessDriver-updater-0-executor:c.c.c.i.a.r.f.FlowletProcessDriver@279] - Transaction operation failed: Conflict detected for transaction 1422334820080000000.
co.cask.tephra.TransactionConflictException: Conflict detected for transaction 1422334820080000000.
	at co.cask.tephra.TransactionContext.checkForConflicts(TransactionContext.java:166) ~[tephra-core-0.3.4.jar:na]
	at co.cask.tephra.TransactionContext.finish(TransactionContext.java:78) ~[tephra-core-0.3.4.jar:na]
```

Note that in order to see this happen, you need to delete the ``profiles`` dataset before redeploying
the application, to force its recreation with the new properties.





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
