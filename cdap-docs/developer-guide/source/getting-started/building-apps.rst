.. :author: Cask Data, Inc.
   :copyright: Copyright © 2014 Cask Data, Inc.

.. _cdap-building-running:

============================================
Building and Running CDAP Applications
============================================

.. highlight:: console

In the examples, we refer to the Standalone CDAP as "CDAP", and the
example code that is running on it as an "Application".


Building an Example Application
+++++++++++++++++++++++++++++++

From the example's project root, build an example with the
`Apache Maven <http://maven.apache.org>`__ command::

	$ mvn clean package


Starting CDAP
+++++++++++++++++++++++++++++++

Before running an Example Applications, check that an instance of CDAP is running and available; if not
follow the instructions for :ref:`Starting and Stopping Standalone CDAP. <start-stop-cdap>`

If you can reach the CDAP Console through a browser at `http://localhost:9999/ <http://localhost:9999/>`__, CDAP is running.


Deploying an Application
+++++++++++++++++++++++++++++++

Once CDAP is started, you can deploy the example JAR by any of these methds:

- Dragging and dropping the application JAR file (``example/target/<example>-<version>.jar``) onto the CDAP Console
  running at `http://localhost:9999/ <http://localhost:9999/>`__; or
- Use the *Load App* button found on the *Overview* of the CDAP Console to browse and upload the Jar; or
- From the example's project root run the App Manager script:

  - Linux: ``$./bin/app-manager.sh --action deploy``
  - Windows: ``>bin\app-manager.bat deploy``


Starting an Application
+++++++++++++++++++++++++++++++

Once the application is deployed:

- You can go to the Application view by clicking on the Application's name. Now you can *Start* or *Stop* the Process
  and Query of the application; or
- From the example's project root run the App Manager script:

  - Linux: ``$./bin/app-manager.sh --action start``
  - Windows: ``>bin\app-manager.bat start``



Stopping an Application
+++++++++++++++++++++++++++++++

Once the application is deployed:

- On the Application detail page of the CDAP Console, click the *Stop* button on both the Process and Query lists; or
- From the example's project root run the App Manager script:

  - Linux: ``$./bin/app-manager.sh --action stop``
  - Windows: ``>bin\app-manager.bat stop``
