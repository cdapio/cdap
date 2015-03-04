.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _cdap-building-running:

============================================
Building and Running CDAP Applications
============================================

.. |example| replace:: <example>

.. highlight:: console

In the examples, we refer to the Standalone CDAP as "CDAP", and the
example code that is running on it as an "Application".


Building an Example Application
----------------------------------

From the example's project root, build an example with the
`Apache Maven <http://maven.apache.org>`__ command::

	$ mvn clean package


Starting CDAP
----------------------------------

Before running an Example Applications, check that an instance of CDAP is running and available; if not
follow the instructions for :ref:`Starting and Stopping Standalone CDAP. <start-stop-cdap>`

If you can reach the CDAP Console through a browser at `http://localhost:9999/ <http://localhost:9999/>`__, CDAP is running.


Deploying an Application
----------------------------------

Once CDAP is started, you can deploy an example JAR by any of these methods:

- Dragging and dropping the application JAR file:

  .. parsed-literal::
    example/target/|example|-|release|.jar
 
  onto the CDAP Console running at `http://localhost:9999/ <http://localhost:9999/>`__; or
- Use the *Load App* button found on the *Overview* of the CDAP Console to browse and upload the Jar; or
- From the Standalone CDAP SDK directory, use the :ref:`Command Line Interface (CLI) <cli>`:

  .. list-table::
    :widths: 20 80
    :stub-columns: 1

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh deploy app <path-to-jar-file>``
    * - On Windows:
      - ``> bin\cdap-cli.bat deploy app <path-to-jar-file>``

Starting an Application
----------------------------------

Once an application is deployed:

- You can go to the Application's detail page in the CDAP Console by clicking on the
  Application's name in the *Overview* page. (It can be reached by clicking on the
  *Application* button in the left sidebar of the window.) Now you can *Start* or *Stop* any
  of the Processes or Queries associated with the application; or
- From the Standalone CDAP SDK directory, use the :ref:`Command Line Interface<cli>`.
  In each CDAP example, the CLI commands for that particular example are provided.

  .. list-table::
    :widths: 20 80
    :stub-columns: 1

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh start <program-type> <app-id.program-id>``
    * - On Windows:
      - ``> bin\cdap-cli.bat start <program-type> <app-id.program-id>``    

  .. list-table::
    :widths: 20 80
    :header-rows: 1

    * - Parameter
      - Description
    * - ``<program-type>``
      - One of ``flow``, ``procedure``, ``mapreduce``, ``workflow`` or ``service``
    * - ``<app-id>``
      - Name of the Application being called
    * - ``<program-id>``
      - Name of the *Flow*, *Procedure*, *MapReduce*, *Workflow*, or *Custom Service*
        being called

Stopping an Application
----------------------------------

Once an application is deployed:

- On the Application's detail page in the CDAP Console, you can click the *Stop* button on 
  the Process and Query lists, if the application has either of them; or
- From the Standalone CDAP SDK directory, use the :ref:`Command Line Interface: <cli>`

  .. list-table::
    :widths: 20 80
    :stub-columns: 1

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh stop <program-type> <app-id.program-id>``
    * - On Windows:
      - ``> bin\cdap-cli.bat stop <program-type> <app-id.program-id>``

Removing an Application
----------------------------------

Once an application is stopped—all Processes (Flows, MapReduce programs, Workflows,
etc.), Queries, and Services are stopped—you can click the *Delete* button on the
Application's detail page in the CDAP Console to delete the Application. After
confirmation, the application will be deleted.

Note that any Storage (Datasets) created or used by the Application will remain, as they
are independent of the Application. Datasets can be deleted with the 
:ref:`HTTP Restful API <restful-api>`, the 
:ref:`Java Client API <java-client-api>`, or the 
:ref:`Command Line Interface API <cli>`.
