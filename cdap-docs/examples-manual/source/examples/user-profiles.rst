.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform WordCount Application
    :copyright: Copyright © 2015-2017 Cask Data, Inc.

.. _examples-user-profiles:

=============
User Profiles
=============

A Cask Data Application Platform (CDAP) example demonstrating column-level conflict
detection in datasets using the example of managing user profiles in a Table.


Overview
========
This application demonstrates the use of the column-level conflict detection in a dataset,
through the example of an application that manages user profiles in a Table.
The fields of a user profile are updated in different ways:

  - Attributes such as name and email address are changed through a RESTful call when the
    user updates their profile.
  - The time of the last login is updated by a sign-on service every
    time the user logs in, also through a RESTful call.
  - The time of the last activity is updated by a flow that processes
    events whenever it encounters an event from that user.

This application illustrates both row-level and column-level conflict detection for a Table.

Let's look at some of these components, and then run the application and see the results.


Introducing Column-Level Conflict Detection
===========================================
As in the other :ref:`examples <examples-index>`, the components
of the application are tied together by a class ``UserProfiles``:

.. literalinclude:: /../../../cdap-examples/UserProfiles/src/main/java/co/cask/cdap/examples/profiles/UserProfiles.java
    :language: java
    :lines: 34-

This application uses a Table with conflict detection either at the row level or
at the column level.

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
system and thus can impact performance.

Column-level conflict detection should be enabled if it is known that different
transactions frequently modify different columns of the same row concurrently.

UserProfiles Application
------------------------
This application uses:

- a stream ``events`` to receive events of user activity;
- a dataset ``profiles`` to store user profiles with conflict detection at either the row or column level;
- a dataset ``counters`` to count events by URL (this is not essential for the purpose of the example);
- a service ``UserProfileService`` to create, delete, and update profiles; and
- a flow ``ActivityFlow`` to count events and record the time of last activity for the users.

The ``UserProfileService`` is a service for creating and modifying user profiles. It has
handlers to create, update, and retrieve user profiles.

A script (``add-users.sh``) is used to populate the ``profiles`` dataset. Two additional
scripts (``update-login.sh`` and ``send-events.sh``) are used to create a conflict by attempting
to write to two different columns of the same row at the same time.


.. Building and Starting
.. =====================
.. |example| replace:: UserProfiles
.. |example-italic| replace:: *UserProfiles*
.. |application-overview-page| replace:: :cdap-ui-apps-programs:`application overview page, programs tab <UserProfiles>`

.. include:: _includes/_building-starting-running.txt


Running the Example
===================

Observing Conflict Detection
----------------------------
To observe conflict detection at both the row-level and column-level, you will need to modify
and build this example twice:

- The first time, you will use row-level conflict detection, and see errors appearing in a log;
- The second time, you will use column-level conflict detection and see the scripts complete successfully without errors.

#. Build the Application with Row-level Conflict Detection

   Before building the application, set the ``ConflictDetection`` appropriately in the class ``UserProfiles``:

   .. literalinclude:: /../../../cdap-examples/UserProfiles/src/main/java/co/cask/cdap/examples/profiles/UserProfiles.java
         :language: java
         :lines: 57-64
         :dedent: 4

   - The first time you build the application, set the ``Table.PROPERTY_CONFLICT_LEVEL`` to
     ``ConflictDetection.ROW``.

   - Build the example (as described :ref:`Building an Example Application <cdap-building-running-example>`).
   - Start CDAP, deploy and start the application and its component.
     Make sure you start the flow and service as described below.
   - Once the application has been deployed and started, you can run the example by `starting the flow and service. <#starting-the-flow>`__
   - You should observe errors as described below, in the ``<CDAP-SDK-home>/logs/cdap-debug.log``.

#. Re-build the Application with Column-level Conflict Detection

   - Stop the application's flow and service (as described `below <#stopping-and-removing-the-application>`__).
   - :ref:`Delete the existing dataset <user-profiles-delete-dataset>` ``profiles``,
     either through the CDAP Command Line Interface or by making a ``curl`` call.
   - Now, rebuild the application, setting the ``Table.PROPERTY_CONFLICT_LEVEL`` back to its
     original value, ``ConflictDetection.COLUMN``.
   - Re-deploy and re-run the application. You should not see any errors in the log.

.. Starting the Flow
.. -----------------
.. |example-flow| replace:: ActivityFlow
.. |example-flow-italic| replace:: *ActivityFlow*

.. include:: _includes/_starting-flow.txt

.. Starting the Service
.. --------------------
.. |example-service| replace:: UserProfileService
.. |example-service-italic| replace:: *UserProfileService*

.. include:: _includes/_starting-service.txt

Populate the *profiles* Table
-----------------------------
Populate the ``profiles`` table with users using a script. From the CDAP Local Sandbox home directory, use:

.. tabbed-parsed-literal::

  $ ./examples/UserProfiles/bin/add-users.sh

Create a Conflict
-----------------
Now, from two different terminals, run the following commands concurrently
(they are set to run, by default, for 100 seconds):

- To randomly update the time of last login for users:

  .. tabbed-parsed-literal::

    .. Linux

    $ ./examples/UserProfiles/bin/update-login.sh

    .. Windows

    > .\examples\UserProfiles\bin\update-login.bat 100 1

- To generate random user activity events and send them to the stream:

  .. tabbed-parsed-literal::

    .. Linux

    $ ./examples/UserProfiles/bin/send-events.sh

    .. Windows

    > .\examples\UserProfiles\bin\send-events.bat 100 1

If both scripts are running at the same time, then some user profiles will be updated at
the same time by the service and by the flow. With row-level conflict detection, you would
see transaction conflicts in the logs. But when the ``profiles`` table uses
column-level conflict detection, these conflicts are avoided.

To see the behavior with row-level conflict detection, set the dataset creation statement
at the bottom of ``UserProfiles.java`` to use ``ConflictDetection.ROW.name()`` and re-run the
steps as above. You should see transaction conflicts in the logs. (One of the scripts will
stop when a conflict occurs. You can stop the other one at that time.)

For example, such a conflict would show as (reformatted to fit)::

  2015-XX-XX 13:22:30,520 - ERROR [executor-
  7:c.c.c.e.p.UserProfileService$UserProfileServiceHandlera910e557f239fd6b95a3ded5c922df3a@-1] - Transaction Failure:
  co.cask.tephra.TransactionConflictException: Conflict detected for transaction 1432066950514000002. at
  co.cask.tephra.TransactionContext.checkForConflicts(TransactionContext.java:174) ~[co.cask.tephra.tephra-core-0.4.1.jar:na] at
  co.cask.tephra.TransactionContext.finish(TransactionContext.java:79) ~[co.cask.tephra.tephra-core-0.4.1.jar:na] at
  . . .

(The log file is located at ``<CDAP-SDK-HOME>/logs/cdap-debug.log``. You should also see
an error in the CDAP UI, in the :cdap-ui-apps:`UserProfileService error log
<UserProfiles/programs/services/UserProfileService/logs?filter=error>`.)

Note that in order to see this happen (and to change from row- to column- and vice-versa),
you need to :ref:`delete the existing dataset <user-profiles-delete-dataset>` ``profiles``
before redeploying the application, to force its recreation with the new properties.

Running the example with ``ConflictDetection.COLUMN.name()`` will result in the two scripts running
concurrently without transaction conflicts.

.. _user-profiles-delete-dataset:

Deleting any Existing *profiles* Dataset
----------------------------------------
If the ``profiles`` dataset has been created from an earlier deployment of the application
and running of the example, it needs to be removed before the next deployment and running,
so that it is created with the correct properties.

To delete the ``profiles`` dataset, either use the CDAP Command Line Interface:

.. tabbed-parsed-literal::

  $ cdap cli delete dataset instance profiles

or by making a ``curl`` call:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X DELETE "http://localhost:11015/v3/namespaces/default/data/datasets/profiles"

Then re-deploy the application.

.. Stopping and Removing the Application
.. =====================================
.. include:: _includes/_stopping-flow-service-removing-application.txt
