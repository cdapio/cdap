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

From the project root, build example with the
`Apache Maven <http://maven.apache.org>`__ command::

	$ mvn clean package

Deploying and Starting the Application
......................................

Make sure an instance of the CDAP is running and available.
From within the SDK root directory, this command will start CDAP in local mode::

	$ ./bin/cdap.sh start

On Windows::

	~SDK> bin\cdap.bat start


Once Started, you can deploy the example JAR by:

#. Dragging and dropping the Application .JAR file (``target/<example>-<version>.jar``)
   onto CDAP Console running at (`http://localhost:9999/ <http://localhost:9999/>`__ in local mode)
   Alternatively, use the *Load App* button found on the *Overview* of the CDAP Console.

On Windows:

#. To deploy the App JAR file, run ``~SDK> bin\app-manager.bat deploy`` or drag and drop the
   Application .JAR file (``target/HelloWorld-<version>.jar`` onto onto CDAP Console
   running at (`http://localhost:9999/ <http://localhost:9999/>`__ in local mode).

#. To start the App, run ``~SDK> bin\app-manager.bat start``

Once the application is deployed, you can go to the Application view by clicking on the Application's Name.
Now you can **Start** or **Stop** Process and Query components available to the application.

Stopping the Application
........................

Either:

- On the Application detail page of the CDAP Console,
  click the *Stop* button on **both** the *Process* and *Query* lists;
or:

#. Linux: ``$./bin/app-manager.sh --action stop``
#. Windows: ``~SDK>bin\app-manager.bat stop``

.. highlight:: java

