:orphan:

.. :Author: Cask Data, Inc.
   :Description: Cask Data Application Platform WordCount Application
   :copyright: Copyright © 2014 Cask Data, Inc.

.. _convention:

Building and Running Applications
---------------------------------

.. highlight:: console

In the examples, we refer to the Standalone CDAP as "CDAP", and the
example code that is running on it as an "Application".

We show the Windows prompt as ``~SDK>`` to indicate a command prompt opened in the SDK directory.

Building the Application
........................

From the project root, build an example with the
`Apache Maven <http://maven.apache.org>`__ command::

	$ mvn clean package

Starting CDAP
.............

Make sure an instance of CDAP is running and available.
From within the SDK root directory, this command will start the Standalone CDAP::

	$ ./bin/cdap.sh start

On Windows::

	~SDK> bin\cdap.bat start


Deploying an application
........................


Once CDAP is Started, you can deploy the example JAR by:

#. Dragging and dropping the application JAR file (``example/target/<example>-<version>.jar``) onto the CDAP Console running at `http://localhost:9999/ <http://localhost:9999/>`__
#. Use the *Load App* button found on the *Overview* of the CDAP Console to browse and upload the Jar.
#. Run the App Manager script located in ``example/bin``:

   - Linux: ``./app-manager.sh --action deploy``
   - Windows: ``app-manager.bat deploy``

Starting an application
.......................

Once the application is deployed,

#. You can go to the Application view by clicking on the Application's Name. Now you can **Start** or **Stop** Process and Query components available to the application.
#. Alternately you can run ``./app-manager.sh --action start`` - on Linux and run ``app-manager.bat start`` on Windows,
   to start the flows and procedures.

.. _stop-application:
Stopping the Application
........................

- On the Application detail page of the CDAP Console, click the *Stop* button on **both** the *Process* and *Query* lists or
- Run the App Manager script:

  - Linux: ``$./bin/app-manager.sh --action stop``
  - Windows: ``~SDK>bin\app-manager.bat stop``

.. highlight:: java

