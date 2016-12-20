.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2016 Cask Data, Inc.

.. _cdap-building-running:

======================================
Building and Running CDAP Applications
======================================

.. |example| replace:: <example>
.. |example-dir| replace:: <example-directory>

.. |development| replace:: *Development Home*
.. _development: http://localhost:11011/oldcdap/ns/default

.. |all_apps| replace:: *All Applications*
.. _all_apps: http://localhost:11011/oldcdap/ns/default/apps

.. |management| replace:: *Management Applications*
.. _management: http://localhost:11011/oldcdap/admin/namespace/detail/default/apps

.. |datasets| replace:: *Management Datasets*
.. _datasets: http://localhost:11011/oldcdap/admin/namespace/detail/default/data

.. |home-page| replace:: *Home*
.. _home-page: http://localhost:11011/cdap/ns/default

.. |management-page| replace:: *Management*
.. _management-page: http://localhost:11011/cdap/management



.. highlight:: console

In the examples, we refer to the Standalone CDAP as *CDAP*, and the example code that is
running on it as an *application*. We'll assume that you are running your application in
the *default* :ref:`namespace <namespaces>`; if not, you will need to adjust commands
accordingly. For example, in a URL such as::

	http://localhost:11015/v3/namespaces/default/apps...

to use the namespace *my_namespace*, you would replace ``default`` with ``my_namespace``::

	http://localhost:11015/v3/namespaces/my_namespace/apps...


Accessing CLI, curl, and the SDK bin
------------------------------------ 

- For brevity in the commands given below, we will simply use ``cdap cli`` for the CDAP
  Command Line Interface. Substitute the actual path of ``./<CDAP-SDK-HOME>/bin/cdap cli``,
  or ``<CDAP-SDK-HOME>\bin\cdap.bat cli`` on Windows, as appropriate. 

- A Windows-version of the application ``curl`` is included in the CDAP SDK as
  ``libexec\bin\curl.exe``; use it as a substitute for ``curl`` in examples.

- If you add the SDK bin directory to your path, you can simplify the commands. From within
  the ``<CDAP-SDK-HOME>`` directory, enter:
  
  .. tabbed-parsed-literal::
  
    .. Linux

    $ export PATH=${PATH}:\`pwd\`/bin

    .. Windows

    > set path=%PATH%;%CD%\bin;%CD%\libexec\bin
  
  The Windows path has been augmented with a directory where the SDK includes
  Windows-versions of commands such as ``curl``.
  
.. include:: /_includes/windows-note.txt

.. _cdap-building-running-example:

Building an Example Application Artifact
----------------------------------------

From the example's project root (such as ``examples/<example-dir>``), build an example with the
`Apache Maven <http://maven.apache.org>`__ command:

.. tabbed-parsed-literal::

  $ mvn clean package

To build without running tests, use:

.. tabbed-parsed-literal::

  $ mvn clean package -DskipTests

To build all the examples, switch to the main examples directory and run the Maven command:

.. tabbed-parsed-literal::

  $ cd <CDAP-SDK-HOME>/examples
  $ mvn clean package -DskipTests


.. _cdap-building-running-starting:

Starting CDAP
-------------

Before running an example application, check that an instance of CDAP is running and available; if not,
follow the instructions for :ref:`Starting and Stopping Standalone CDAP. <start-stop-cdap>`

If you can reach the CDAP UI through a browser at :cdap-ui:`http://localhost:11011/ <>`,
CDAP is running.

.. _cdap-building-running-deploying:

Deploying an Application
------------------------

Once CDAP is started, you can deploy an application using an example JAR by any of these methods:

- In the CDAP UI, use the *Add App* button |---| found on either the |development|_ or 
  |management|_ pages |---| selecting the *Custom App* menu item, and then browse and upload an
  artifact to create an app:

  .. tabbed-parsed-literal::

    examples/|example-dir|/target/|example|-|release|.jar

- In the :ref:`new beta CDAP UI <cdap-ui-beta>`,  use the green "plus" button |---| found on either
  the |home-page|_ or |management-page|_ pages |---| to bring up the *Cask Market/Resource
  Center*. Click the button to change to the *Resource Center* if it is not already
  visible. Click the *Upload* button on the *Application* card, and then either
  drag-and-drop the JAR file to be uploaded or browse and upload the artifact to create an app:

  .. tabbed-parsed-literal::

    examples/|example-dir|/target/|example|-|release|.jar
 
- From the Standalone CDAP SDK directory, use the :ref:`Command Line Interface (CLI) <cli>`:

  .. tabbed-parsed-literal::

      $ cdap cli load artifact examples/|example-dir|/target/|example|-|release|.jar
      Successfully added artifact with name '|example|'

      $ cdap cli create app <app name> |example| |release| user
      Successfully created application

  The CLI can be accessed under Windows using the ``bin\cdap.bat cli`` script.
  
- Use an application such as ``curl`` (a Windows-version is included in the CDAP SDK in
  ``libexec\bin\curl.exe``):

  .. tabbed-parsed-literal::
  
    $ curl -w"\n" localhost:11015/v3/namespaces/default/artifacts/|example| \
      --data-binary @examples/|example-dir|/target/|example|-|release|.jar
    Artifact added successfully

    $ curl -w"\n" -X PUT -H "Content-Type: application/json" localhost:11015/v3/namespaces/default/apps/<app name> \
      -d '{ "artifact": { "name": "|example|", "version": "|release|", "scope": "user" }, "config": {} }'
    Deploy Complete


.. _cdap-building-running-starting-application:
.. _cdap-building-running-starting-programs:

Starting an Application's Programs
----------------------------------

Once an application is deployed, there are multiple methods for starting an application's programs:

- You can go to the application's detail page in the CDAP UI by clicking on the
  application's name in either the |development|_ page or on the |all_apps|_ page. Now you can 
  see the status of any of the programs associated with the application and, by clicking
  on them, go to their detail page where you can start or stop them.
  
- From the Standalone CDAP SDK directory, use the :ref:`Command Line Interface<cli>` to start a specific program of an application.
  (In each CDAP example, the CLI commands for that particular example are provided):
  
  .. tabbed-parsed-literal::

    $ cdap cli start <program-type> <app-id.program-id>
    
  .. list-table::
    :widths: 20 80
    :header-rows: 1

    * - Parameter
      - Description
    * - ``<program-type>``
      - One of ``flow``, ``mapreduce``, ``service``, ``spark``, ``worker``, or ``workflow``
    * - ``<app-id>``
      - Name of the application being called
    * - ``<program-id>``
      - Name of the *flow*, *MapReduce*, *service*, *spark*, *worker* or *workflow* being called

..

- Use the :ref:`Command Line Interface<cli>` to start all or selected types of programs of an application at once:
  
  .. tabbed-parsed-literal::

    $ start app <app-id> programs [of type <program-types>]
    
  .. list-table::
    :widths: 20 80
    :header-rows: 1

    * - Parameter
      - Description
    * - ``<app-id>``
      - Name of the application being called
    * - ``<program-types>``
      - An optional comma-separated list of program types (``flow``, ``mapreduce``, ``service``,
        ``spark``, ``worker``, or ``workflow``) which will start all programs of those
        types; for example, specifying ``'flow,workflow'`` will start all flows and
        workflows in the application
  
..
      
- Use the :ref:`Program Lifecycle <http-restful-api-lifecycle-start>` of the Lifecycle
  HTTP RESTful API to start the programs of an application using a program such as ``curl``

.. _cdap-building-running-stopping:
.. _cdap-building-running-stopping-application:
.. _cdap-building-running-stopping-program:

Stopping an Application's Programs
----------------------------------

Once an application is deployed, use one of these methods for stoping an application's programs:

- On an application's detail page in the CDAP UI, you can click on a program to go 
  to its detail page and then click the *Stop* button there

- From the Standalone CDAP SDK directory, use the :ref:`Command Line Interface <cli>` to stop a specific program of an application:

  .. tabbed-parsed-literal::

    $ cdap cli stop <program-type> <app-id.program-id>

  .. list-table::
    :widths: 20 80
    :header-rows: 1

    * - Parameter
      - Description
    * - ``<program-type>``
      - One of ``flow``, ``mapreduce``, ``service``, ``spark``, ``worker``, or ``workflow``
    * - ``<app-id>``
      - Name of the application being called
    * - ``<program-id>``
      - Name of the *flow*, *MapReduce*, *service*, *spark*, *worker* or *workflow* being called

..

- Use the :ref:`Command Line Interface<cli>` to stop all or selected types of programs of an application at once:
  
  .. tabbed-parsed-literal::

    $ stop app <app-id> programs [of type <program-types>]
    
  .. list-table::
    :widths: 20 80
    :header-rows: 1

    * - Parameter
      - Description
    * - ``<app-id>``
      - Name of the application being called
    * - ``<program-types>``
      - An optional comma-separated list of program types (``flow``, ``mapreduce``, ``service``,
        ``spark``, ``worker``, or ``workflow``) which will stop all programs of those
        types; for example, specifying ``'flow,workflow'`` will stop all flows and
        workflows in the application

..
      
- Use the :ref:`Program Lifecycle <http-restful-api-lifecycle-stop>` of the Lifecycle
  HTTP RESTful API to stop the programs of an application using a program such as ``curl``
    
.. _cdap-building-running-removing:

Removing an Application
-----------------------

Once an application is "stopped" |---| when all of its programs (flows, MapReduce programs,
workflows, services, etc.) are stopped |---| you can go to the |all_apps|_
page of the CDAP UI, click on the particular application to go to its detail page, click
the *Actions* menu on the right side and select *Manage* to go to the Management pane for
the application, then click the *Actions* menu on the right side and select *Delete*.

After confirmation, the application will be deleted.

From the Standalone CDAP SDK directory, you can also use the Command Line Interface:

.. tabbed-parsed-literal::

  $ cdap cli delete app <app-id>

Note that any storage (datasets) created or used by the application will remain, as they
are independent of the application. Datasets can be deleted from the |datasets|_ page of
the CDAP UI, or by using the :ref:`HTTP Restful API <restful-api>`, the 
:ref:`Java Client API <java-client-api>`, or the :ref:`Command Line Interface API <cli>`.

Streams can be either truncated or deleted, using similar methods.

The artifact used to create the application will also remain, as multiple
applications can be created from the same artifact. Artifacts can be deleted using the
:ref:`Http Restful API <restful-api>`, the
:ref:`Java Client API <java-client-api>`, or the :ref:`Command Line Interface API <cli>`.
