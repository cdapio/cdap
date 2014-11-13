.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform Hello World Application
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _examples-hello-world:

===========
Hello World
===========

The simplest Cask Data Application Platform (CDAP) Example.

Overview
===========

This application uses one Stream, one Dataset, one Flow and one Procedure to implement the classic "Hello World".

- A stream to send names to;
- A flow with a single flowlet that reads the stream and stores in a dataset each name in a KeyValueTable; and
- A procedure that reads the name from the KeyValueTable and prints 'Hello [Name]!'.


The ``HelloWorld`` Application
-------------------------------

.. literalinclude:: /../../../cdap-examples/HelloWorld/src/main/java/co/cask/cdap/examples/helloworld/HelloWorld.java
   :language: java
   :lines: 45-55
   
The application uses a stream called *who* to ingest data through a flow *WhoFlow* to a dataset *whom*.

The ``WhoFlow``
---------------

This is a trivial flow with a single flowlet named *saver* of type ``NameSaver``:

.. literalinclude:: /../../../cdap-examples/HelloWorld/src/main/java/co/cask/cdap/examples/helloworld/HelloWorld.java
   :language: java
   :lines: 60-71
   
The flowlet uses a dataset of type ``KeyValueTable`` to store the names it reads from the stream. Every time a new
name is received, it is stored in the table under the key ``name``-and it overwrites any name that was previously
stored:

.. literalinclude:: /../../../cdap-examples/HelloWorld/src/main/java/co/cask/cdap/examples/helloworld/HelloWorld.java
   :language: java
   :lines: 76-95
  
Note that the flowlet also emits metrics: every time a name longer than 10 characters is received,
the counter ``names.longnames`` is incremented by one, and the metric ``names.bytes`` is incremented
by the length of the name. We will see below how to retrieve these metrics using the 
:ref:`http-restful-api-metrics`.

The ``Greeting`` Procedure
------------------------------

This procedure has a single handler method called ``greet`` that does not accept arguments. When invoked, it
reads the name stored by the ``NameSaver`` from the key-value table. It return a simple greeting with that name:

.. literalinclude:: /../../../cdap-examples/HelloWorld/src/main/java/co/cask/cdap/examples/helloworld/HelloWorld.java
   :language: java
   :lines: 100-115

Note that the procedure, like the flowlet, also emits metrics: every time the name *Jane
Doe* is received, the counter ``greetings.count.jane_doe`` is incremented by one. We will
see below how to retrieve this metric sing the 
:ref:`http-restful-api-metrics`.

Building and Starting
=================================

- You can either build the example (as described `below
  <#building-an-example-application>`__) or use the pre-built JAR file included in the CDAP SDK.
- Start CDAP, deploy and start the application as described below in 
  `Running CDAP Applications`_\ .
  Make sure you start the flow and procedure as described.
- Once the application has been deployed and started, you can `run the example. <#running-the-example>`__

Running CDAP Applications
============================================

.. include:: /../../developers-manual/source/getting-started/building-apps.rst
   :start-line: 9

Running the Example
===================

Injecting a Name
------------------------------

In the Application's detail page, under *Process*, click on *WhoFlow*. This takes you to
the flow details page. Click on the *Start* button in the right-side, below the green
arrow. The flow's label will change to *Running* when it is ready to receive events.

Now click on the *who* stream on the left side of the flow visualization, which brings up
a pop-up window. Enter a name and click the *Inject* button. After you close the pop-up
window, you will see that the counters for both the stream and the *saver* flowlet
increase to 1. You can repeat this step to enter more names, but remember that only the
last name is stored in the key-value table.

Using the Procedure
------------------------------

Go back to the Application's detail page, and under Query, click on the *Greeting*
procedure. Click on the *Start* button in the right-side, below the green arrow. The
procedure's label will change to *Running* when it is ready to receive events.

Now you can make a request to the procedure: Enter "greet" for the method and click the
Execute button. At the bottom of the page you will see the procedure's response. If the
last name you entered was *Tom*, this will be "Hello, Tom!".
  
Retrieving Metrics
------------------------------

.. highlight:: console

You can now query the metrics that are emitted by the flow. To see the value of the ``names.bytes`` metric,
you can make an HTTP request to the :ref:`http-restful-api-metrics` using curl::

  $ curl http://localhost:10000/v2/metrics/user/apps/HelloWorld/flows/WhoFlow/flowlets/saver/names.bytes?aggregate=true; echo
  {"data":3}

To see the value of the ``names.longnames`` metric (the number of names greater than 10 characters),
you can use::

  $ curl http://localhost:10000/v2/metrics/user/apps/HelloWorld/flows/WhoFlow/flowlets/saver/names.longnames?aggregate=true; echo
  {"data":2}
  
To see the value of the ``greetings.count.jane_doe`` metric (the number of times the name *Jane Doe* has been seen),
you can use::

  $ curl http://localhost:10000/v2/metrics/user/apps/HelloWorld/procedures/Greeting/greetings.count.jane_doe?aggregate=true; echo
  {"data":0}
  
Stopping the Application
-------------------------------
Once done, you can stop the application as described above in `Stopping an Application. <#stopping-an-application>`__
