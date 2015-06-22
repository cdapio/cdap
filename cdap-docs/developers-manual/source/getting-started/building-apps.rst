.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _cdap-building-running:

======================================
Building and Running CDAP Applications
======================================

.. |example| replace:: <example>

.. |develop| replace:: *Development*
.. _develop: http://localhost:9999/ns/default

.. |all_apps| replace:: *All Applications*
.. _all_apps: http://localhost:9999/ns/default/apps

.. |management| replace:: *Management*
.. _management: http://localhost:9999/admin/namespace/detail/default/apps

.. |datasets| replace:: *Datasets*
.. _datasets: http://localhost:9999/admin/namespace/detail/default/data


.. highlight:: console

In the examples, we refer to the Standalone CDAP as *CDAP*, and the example code that is
running on it as an *application*. We'll assume that you are running your application in
the *default* :ref:`namespace <namespaces>`; if not, you will need to adjust commands
accordingly. For example, in a URL such as::

	http://localhost:10000/v3/namespaces/default/apps...

to use the namespace *my_namespace*, you would replace ``default`` with ``my_namespace``::

	http://localhost:10000/v3/namespaces/my_namespace/apps...


Accessing CLI, curl and the SDK bin
----------------------------------- 

- For brevity in the commands given below, we will simply use ``cdap-cli.sh`` for the CDAP
  Command Line Interface. Substitute the actual path of ``./<CDAP-SDK-HOME>/bin/cdap-cli.sh``,
  or ``<CDAP-SDK-HOME>\bin\cdap-cli.bat`` on Windows, as appropriate. 

- A Windows-version of the application ``curl`` is included in the CDAP SDK as
  ``libexec\bin\curl.exe``; use it as a substitute for ``curl`` in the examples shown below.

- If you add the SDK bin directory to your path, you can simplify the commands. From within
  the CDAP-SDK-home directory, enter::

    $ export PATH=${PATH}:`pwd`/bin

  or under Windows::

    > setx path "%PATH%;%CD%\bin"
  
  Note that under Windows, you'll need to create a new command line window in order to see
  this change to the path variable.

Building an Example Application
-------------------------------

From the example's project root, build an example with the
`Apache Maven <http://maven.apache.org>`__ command::

	$ mvn clean package


Starting CDAP
-------------

Before running an example application, check that an instance of CDAP is running and available; if not,
follow the instructions for :ref:`Starting and Stopping Standalone CDAP. <start-stop-cdap>`

If you can reach the CDAP UI through a browser at `http://localhost:9999/ <http://localhost:9999/>`__, 
CDAP is running.

Deploying an Application
------------------------

Once CDAP is started, you can deploy an example JAR by any of these methods:

.. - Dragging and dropping the application JAR file:

  .. parsed-literal::
    examples/|example|/target/|example|-|release|.jar
 
..  onto the CDAP UI running at `http://localhost:9999/ <http://localhost:9999/>`__; or

- Use the *Add App* button found on the |management|_ page of the CDAP UI to browse and upload the Jar:

  .. parsed-literal::
    examples/|example|/target/|example|-|release|.jar
 
- From the Standalone CDAP SDK directory, use the :ref:`Command Line Interface (CLI) <cli>`:

  .. container:: highlight

    .. parsed-literal::
      |$| cdap-cli.sh deploy app examples/|example|/target/|example|-|release|.jar
    
      Successfully deployed application

  The CLI can be accessed under Windows using the ``bin\cdap-cli.bat`` script.
  
- Use an application such as ``curl`` (a Windows-version is included in the CDAP SDK in
  ``libexec\bin\curl.exe``):

  .. container:: highlight
  
    .. parsed-literal::
      |$| curl -w'\\n' -H "X-Archive-Name: |example|-|release|.jar" localhost:10000/v3/namespaces/default/apps \\
        --data-binary @examples/|example|/target/|example|-|release|.jar

      Deploy Complete


Starting an Application
-----------------------

Once an application is deployed, there are multiple methods for starting an application:

- You can go to the application's detail page in the CDAP UI by clicking on the
  application's name in either the |develop|_ page or on the |all_apps|_ page. Now you can 
  see the status of any of the programs associated with the application and, by clicking
  on them, go to their detail page where you can start or stop them.
- From the Standalone CDAP SDK directory, use the :ref:`Command Line Interface<cli>`.
  In each CDAP example, the CLI commands for that particular example are provided::

    $ cdap-cli.sh start <program-type> <app-id.program-id>
    
  .. list-table::
    :widths: 20 80
    :header-rows: 1

    * - Parameter
      - Description
    * - ``<program-type>``
      - One of ``adapter``, ``flow``, ``mapreduce``, ``service``, ``spark``, ``worker``, or ``workflow``
    * - ``<app-id>``
      - Name of the application being called
    * - ``<program-id>``
      - Name of the *adapter*, *flow*, *MapReduce*, *service*, *spark*, *worker* or *workflow* being called
      

Stopping an Application
-----------------------

Once an application is deployed:

- On an application's detail page in the CDAP UI, you can click on a program to go 
  to its detail page and then click the *Stop* button there; or
- From the Standalone CDAP SDK directory, use the :ref:`Command Line Interface <cli>`::

    $ cdap-cli.sh stop <program-type> <app-id.program-id>
    
    
Removing an Application
-----------------------

Once an application is stopped |---| all of its programs (flows, MapReduce programs,
workflows, services, etc.) are stopped |---| you can go to the |all_apps|_
page of the CDAP UI, click on the particular application to go to its detail page, click
the *Actions* menu on the right side and select *Manage* to go to the Management pane for
the application, then click the *Actions* menu on the right side and select *Delete*.

After confirmation, the application will be deleted.

From the Standalone CDAP SDK directory, you can also use the Command Line Interface::

    $ cdap-cli.sh delete app <app-id>

Note that any storage (datasets) created or used by the application will remain, as they
are independent of the application. Datasets can be deleted from the |datasets|_ page of
the CDAP UI, or by using the :ref:`HTTP Restful API <restful-api>`, the 
:ref:`Java Client API <java-client-api>`, or the :ref:`Command Line Interface API <cli>`.

Streams can be either truncated or deleted, using similar methods.

