.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform Hello World Example
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _examples-hello-world:

===========
Hello World
===========

The simplest Cask Data Application Platform (CDAP) Example.

Overview
===========

This application uses one Stream, one Dataset, one Flow and one Service to implement the classic "Hello World":

- A stream to send names to;
- A flow with a single flowlet that reads the stream and stores in a dataset each name in a KeyValueTable; and
- A service that reads the name from the KeyValueTable and responds with "Hello [Name]!"


The ``HelloWorld`` Application
-------------------------------

.. literalinclude:: /../../../cdap-examples/HelloWorld/src/main/java/co/cask/cdap/examples/helloworld/HelloWorld.java
   :language: java
   :lines: 40-58
   
The application uses a stream called *who* to ingest data through a flow *WhoFlow* to a dataset *whom*.

The ``WhoFlow``
---------------

This is a trivial flow with a single flowlet named *saver* of type ``NameSaver``:

.. literalinclude:: /../../../cdap-examples/HelloWorld/src/main/java/co/cask/cdap/examples/helloworld/HelloWorld.java
   :language: java
   :lines: 63-74
   
The flowlet uses a dataset of type ``KeyValueTable`` to store the names it reads from the stream. Every time a new
name is received, it is stored in the table under the key ``name``-and it overwrites any name that was previously
stored:

.. literalinclude:: /../../../cdap-examples/HelloWorld/src/main/java/co/cask/cdap/examples/helloworld/HelloWorld.java
   :language: java
   :lines: 79-100
  
Note that the flowlet also emits metrics: every time a name longer than 10 characters is received,
the counter ``names.longnames`` is incremented by one, and the metric ``names.bytes`` is incremented
by the length of the name. We will see below how to retrieve these metrics using the 
:ref:`http-restful-api-metrics`.

The ``Greeting`` Service
------------------------------

This service has a single endpoint called ``greet`` that does not accept arguments. When invoked, it
reads the name stored by the ``NameSaver`` from the key-value table. It return a simple greeting with that name:

.. literalinclude:: /../../../cdap-examples/HelloWorld/src/main/java/co/cask/cdap/examples/helloworld/HelloWorld.java
   :language: java
   :lines: 105-137

Note that the service, like the flowlet, also emits metrics: every time the name *Jane Doe* is received,
the counter ``greetings.count.jane_doe`` is incremented by one.
We will see below how to retrieve this metric using the
:ref:`http-restful-api-metrics`.

Building and Starting
=================================

- You can either build the example (as described `below
  <#building-an-example-application>`__) or use the pre-built JAR file included in the CDAP SDK.
- Start CDAP, deploy and start the application and its components as described below in 
  `Running CDAP Applications`_\ .
  Make sure you start the flow and service as described below.
- Once the application has been deployed and started, you can `run the example. <#running-the-example>`__

Running CDAP Applications
============================================

.. |example| replace:: HelloWorld

.. include:: /../../developers-manual/source/getting-started/building-apps.rst
   :start-line: 11

Running the Example
===================

Starting the Flow
------------------------------

Once the application is deployed:

- Click on the *Process* button in the left sidebar of the CDAP UI,
  then click ``WhoFlow`` in the *Process* page to get to the
  Flow detail page, then click the *Start* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ ./bin/cdap-cli.sh start flow HelloWorld.WhoFlow
  

Starting the Service
------------------------------

Once the application is deployed:

- Click on ``HelloWorld`` in the Overview page of the CDAP UI to get to the
  Application detail page, click ``Greeting`` in the *Service* pane to get to the
  Service detail page, then click the *Start* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ ./bin/cdap-cli.sh start service HelloWorld.Greeting
    
    
Injecting a Name
------------------------------

In the Application's detail page, under *Process*, click on *WhoFlow*. This takes you to
the flow details page. (If you haven't already started the Flow, click on the *Start*
button in the right-side, below the green arrow.) The Flow's label will read *Running*
when it is ready to receive events.

Now click on the *who* Stream on the left side of the flow visualization, which brings up
a pop-up window. Enter a name and click the *Inject* button. After you close the pop-up
window, you will see that the counters for both the Stream and the *saver* Flowlet
increase to 1. You can repeat this step to enter more names, but remember that only the
last name is stored in the key-value table.

Using the Service
------------------------------

Go back to the Application's detail page, and under Service, click on the *Greeting*
service. (If you haven't already started the Service, click on the *Start* button in the
right-side, below the green arrow.) The Service's label will read *Running* when it is
ready to receive events.

Now you can make a request to the service using ``curl``::

  $ curl -w'\n' http://localhost:10000/v3/namespaces/default/apps/HelloWorld/services/Greeting/methods/greet

If the last name you entered was *Tom*, the Service will respond with ``Hello Tom!``

**Note:** A version of ``curl`` that works with Windows is included in the CDAP Standalone
SDK in ``libexec\bin\curl.exe``
  
Retrieving Metrics
------------------------------

.. highlight:: console

You can now query the metrics that are emitted by the Flow and Service. If a particular
metric has no value, it will return an empty array in the ``"series"`` of the results, such
as::

  {"startTime":0,"endTime":1429475995,"series":[]}

To see the value of the ``names.bytes`` metric, you can make an HTTP request to the
:ref:`http-restful-api-metrics` using curl::

  $ curl -w'\n' -X POST 'http://localhost:10000/v3/metrics/query?tag=namespace:default&tag=app:HelloWorld&tag=flow:WhoFlow&tag=flowlet:saver&metric=user.names.bytes&aggregate=true'
  {"startTime":0,"endTime":1429477634,"series":[{"metricName":"user.names.bytes","grouping":{},"data":[{"time":0,"value":44}]}]}

To see the value of the ``names.longnames`` metric (the number of names, each greater than 10 characters),
you can use::

  $ curl -w'\n' -X POST 'http://localhost:10000/v3/metrics/query?tag=namespace:default&tag=app:HelloWorld&tag=flow:WhoFlow&tag=flowlet:saver&metric=user.names.longnames&aggregate=true'
  {"startTime":0,"endTime":1429476082,"series":[{"metricName":"user.names.longnames","grouping":{},"data":[{"time":0,"value":1}]}]}
  
To see the value of the ``greetings.count.jane_doe`` metric (the number of times the name *Jane Doe* has been "greeted"),
you can use::

  $ curl -w'\n' -X POST 'http://localhost:10000/v3/metrics/query?tag=namespace:default&tag=app:HelloWorld&tag=service:Greeting&metric=user.greetings.count.jane_doe&aggregate=true'
  {"startTime":0,"endTime":1429464632,"series":[{"metricName":"user.greetings.count.jane_doe","grouping":{},"data":[{"time":0,"value":0}]}]}

The results you receive will vary depending on the entries you have made to the Flow.

Stopping the Application
-------------------------------
Once done, you can stop the application as described above in `Stopping an Application. 
<#stopping-an-application>`__ Here is an example-specific description of the steps:

**Stopping the Flow**

- Click on the *Process* button in the left sidebar of the CDAP UI,
  then click ``WhoFlow`` in the *Process* page to get to the
  Flow detail page, then click the *Stop* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ ./bin/cdap-cli.sh stop flow HelloWorld.WhoFlow
      

**Stopping the Service**

- Click on ``HelloWorld`` in the Overview page of the CDAP UI to get to the
  Application detail page, click ``Greeting`` in the *Service* pane to get to the
  Service detail page, then click the *Stop* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ ./bin/cdap-cli.sh stop service HelloWorld.Greeting   
