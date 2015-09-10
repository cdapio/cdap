.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform Hello World Example
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _examples-hello-world:

===========
Hello World
===========

The simplest Cask Data Application Platform (CDAP) example.

Overview
========

This application uses one stream, one dataset, one flow and one service to implement the classic "Hello World":

- A stream to send names to;
- A flow with a single flowlet that reads the stream and stores in a dataset each name in a KeyValueTable; and
- A service that reads the name from the KeyValueTable and responds with "Hello [Name]!"


The ``HelloWorld`` Application
------------------------------

.. literalinclude:: /../../../cdap-examples/HelloWorld/src/main/java/co/cask/cdap/examples/helloworld/HelloWorld.java
   :language: java
   :lines: 47-57
   
The application uses a stream called *who* to ingest data through a flow *WhoFlow* to a dataset *whom*.

The ``WhoFlow``
---------------

This is a trivial flow with a single flowlet named *saver* of type ``NameSaver``:

.. literalinclude:: /../../../cdap-examples/HelloWorld/src/main/java/co/cask/cdap/examples/helloworld/HelloWorld.java
   :language: java
   :lines: 62-71
   
The flowlet uses a dataset of type ``KeyValueTable`` to store the names it reads from the stream. Every time a new
name is received, it is stored in the table under the key ``name``, and it overwrites any name that was previously
stored:

.. literalinclude:: /../../../cdap-examples/HelloWorld/src/main/java/co/cask/cdap/examples/helloworld/HelloWorld.java
   :language: java
   :lines: 76-97
  
Note that the flowlet also emits metrics: every time a name longer than 10 characters is received,
the counter ``names.longnames`` is incremented by one, and the metric ``names.bytes`` is incremented
by the length of the name. We will see below how to retrieve these metrics using the 
:ref:`http-restful-api-metrics`.

The ``Greeting`` Service
------------------------

This service has a single endpoint called ``greet`` that does not accept arguments. When invoked, it
reads the name stored by the ``NameSaver`` from the key-value table. It return a simple greeting with that name:

.. literalinclude:: /../../../cdap-examples/HelloWorld/src/main/java/co/cask/cdap/examples/helloworld/HelloWorld.java
   :language: java
   :lines: 102-112

Note that the service, like the flowlet, also emits metrics: every time the name *Jane Doe* is received,
the counter ``greetings.count.jane_doe`` is incremented by one.
We will see below how to retrieve this metric using the
:ref:`http-restful-api-metrics`.


.. |example| replace:: HelloWorld
.. include:: building-starting-running-cdap.txt


Running the Example
===================

Starting the Flow
-----------------

Once the application is deployed:

- Go to the *HelloWorld* `application overview page 
  <http://localhost:9999/ns/default/apps/HelloWorld/overview/status>`__,
  click ``WhoFlow`` to get to the flow detail page, then click the *Start* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh start flow HelloWorld.WhoFlow
  
    Successfully started flow 'WhoFlow' of application 'HelloWorld' with stored runtime arguments '{}'

Starting the Service
--------------------

Once the application is deployed:

- Go to the *HelloWorld* `application overview page 
  <http://localhost:9999/ns/default/apps/HelloWorld/overview/status>`__,
  click ``Greeting`` to get to the service detail page, then click the *Start* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh start service HelloWorld.Greeting
    
    Successfully started service 'Greeting' of application 'HelloWorld' with stored runtime arguments '{}'

Injecting a Name
----------------

.. |who-flow| replace:: *WhoFlow*
.. _who-flow: http://localhost:9999/ns/default/apps/HelloWorld/programs/flows/WhoFlow/runs

In the application's detail page, click on |who-flow|_. This takes you to
the flow details page. (If you haven't already started the flow, click on the *Start*
button in the right-side, below the green arrow.) The flow's *status* will read *RUNNING*
when it is ready to receive events.

Now double-click on the *who* stream on the left side of the flow visualization, which brings up
a pop-up window. Enter a name and click the *Inject* button. After you close the pop-up
window, you will see that the counters for both the stream and the *saver* flowlet
increase to 1. You can repeat this step to enter more names, but remember that only the
last name is stored in the key-value table.

Metrics are collected based on the ``bytes`` metric (the total number of bytes of names),
the ``longnames`` metric (the number of names, each greater than 10 characters), and the
``greetings.count.jane_doe`` metric (the number of times the name *Jane Doe* has been
"greeted").

To try out these metrics, first send a few long names (each greater than 10 characters)
and send *Jane Doe* a number of times.

Using the Service
-----------------

Go back to the application's detail page, and click on the *Greeting* service. (If you
haven't already started the service, click on the *Start* button on the right-side.) The
service's label will read *Running* when it is ready to receive events.

Now you can make a request to the service using ``curl``::

  $ curl -w'\n' http://localhost:10000/v3/namespaces/default/apps/HelloWorld/services/Greeting/methods/greet

If the last name you entered was *Tom*, the service will respond with ``Hello Tom!``

There is a *Make Request* button in the CDAP UI that will make the same request, with a
similar response.

Retrieving Metrics
------------------

.. highlight:: console

You can now query the metrics that are emitted by the flow and service. If a particular
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

The results you receive will vary depending on the entries you have made to the flow.


Stopping and Removing the Application
=====================================
Once done, you can stop the application as described in :ref:`Stopping an Application 
<cdap-building-running-stopping>`. Here is an example-specific description of the steps:

**Stopping the Flow**

- Go to the *HelloWorld* `application overview page 
  <http://localhost:9999/ns/default/apps/HelloWorld/overview/status>`__,
  click ``WhoFlow`` to get to the flow detail page, then click the *Stop* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh stop flow HelloWorld.WhoFlow
    Successfully stopped Flow 'WhoFlow' of application 'HelloWorld'

**Stopping the Service**

- Go to the *HelloWorld* `application overview page 
  <http://localhost:9999/ns/default/apps/HelloWorld/overview/status>`__,
  click ``Greeting`` to get to the service detail page, then click the *Stop* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh stop service HelloWorld.Greeting
    Successfully stopped service 'Greeting' of application 'HelloWorld'

**Removing the Application**

You can now remove the application as described in :ref:`Removing an Application <cdap-building-running-removing>`, or:

- Go to the *HelloWorld* `application overview page 
  <http://localhost:9999/ns/default/apps/HelloWorld/overview/status>`__,
  click the *Actions* menu on the right side and select *Manage* to go to the Management pane for the application,
  then click the *Actions* menu on the right side and select *Delete* to delete the application; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh delete app HelloWorld
