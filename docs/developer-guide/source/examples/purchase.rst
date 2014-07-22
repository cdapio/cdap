.. :Author: Continuuity, Inc.
   :Description: Continuuity Reactor Purchase Application

============================
Purchase Application Example
============================

---------------------------------------------------------------------------
A Continuuity Reactor Application demonstrating all Reactor elements
---------------------------------------------------------------------------

.. reST Editor: .. section-numbering::
.. reST Editor: .. contents::

Overview
========
This example demonstrates use of each of the Reactor elements: Streams, Flows, Flowlets,
Datasets, Queries, Procedures, MapReduce jobs, Workflows, and Custom Services in a single Application.

The application uses a scheduled MapReduce job and Workflow to read from one ObjectStore Dataset
and write to another.

  - Send sentences of the form "Tom bought 5 apples for $10" to the ``purchaseStream``.
    You can send sentences either by using a ``curl`` call, using the ``inject-data`` script
    included in the example's ``/bin`` directory, or using the Continuuity Reactor Dashboard.
  - The ``PurchaseFlow`` reads the ``purchaseStream`` and converts every input String into a
    Purchase object and stores the object in the *purchases* Dataset.
  - The ``CatalogLookupService`` fetches the catalog id for a given product. The CatalogLookupService
    is called from the PurchaseStore Flowlet. The host and port of the CatalogLookupService is discovered
    using the Service discovery framework.
  - When scheduled by the ``PurchaseHistoryWorkFlow``, the ``PurchaseHistoryBuilder`` MapReduce
    job reads the *purchases* Dataset, creates a purchase history, and stores the purchase
    history in the *history* Dataset every morning at 4:00 A.M.
  - You can either manually (in the Process screen of the Reactor Dashboard) or 
    programmatically execute the ``PurchaseHistoryBuilder`` MapReduce job to store 
    customers' purchase history in the *history* Dataset.
  - Execute the ``PurchaseProcedure`` procedure to query the *history* Dataset to discover the
    purchase history of each user.
  - Execute a SQL query over the *history* Dataset. You can do this using a series of ``curl``
    calls, or more conveniently using the ``send-query`` script.

**Note:** Because the PurchaseHistoryWorkFlow is only scheduled to run at 4:00 A.M.,
you should not start it manually until after entering the first customers' purchases, or the
PurchaseProcedure will return a "not found" error.

Let's look at some of these elements, and then run the Application and see the results.

The Purchase Application
------------------------
As in the other `examples <index.html>`__, the components
of the Application are tied together by the class ``PurchaseApp``::

  public class PurchaseApp extends AbstractApplication {

    @Override
    public void configure() {
      setName("PurchaseHistory");
      setDescription("Purchase history app");
      
      // Ingest data into the Application via Streams
      addStream(new Stream("purchaseStream"));
      
      // Store processed data in Datasets
      createDataSet("frequentCustomers", KeyValueTable.class);
      
      // Process events in real-time using Flows
      addFlow(new PurchaseFlow());
      
      // Query the processed data using a Procedure
      addProcedure(new PurchaseProcedure());
      
      // Run a MapReduce job on the acquired data using a Workflow
      addWorkflow(new PurchaseHistoryWorkflow());
      
      // Provide a Service to Application components
      addService(new CatalogLookupService());

      try {
        createDataSet("history", PurchaseHistoryStore.class, PurchaseHistoryStore.properties());
        ObjectStores.createObjectStore(getConfigurer(), "purchases", Purchase.class);
      } catch (UnsupportedTypeException e) {
        // This exception is thrown by ObjectStore if its parameter type cannot be (de)serialized
        // (for example, if it is an interface and not a class), as there is no auto-magic way to
        // de-serialize an object. In this case, that cannot happen because both PurchaseHistoryStore
        // and Purchase are actual classes.
        throw new RuntimeException(e);
      }
    }
  }


``PurchaseHistory`` and ``Purchase``: ObjectStore Data Storage
--------------------------------------------------------------
The raw purchase data is stored in an ObjectStore Dataset, *purchases*,
with this method defined in ``PurchaseStore``::

	process(Purchase purchase)

This method is what actually puts data into the *purchases* Dataset, by writing to the
Dataset with each purchase's timestamp and the ``Purchase`` Object.

The purchase history for each customer is compiled by the ``PurchaseHistoryWorkflow``, which uses a
Map/Reduce job, ``PurchaseHistoryBuilder``, to aggregate all purchases into a per-customer purchase
history. It writes to the *history* Dataset, a custom Dataset that embeds an ``ObjectStore`` and also
implements the ``RecordScannable`` interface to allow SQL queries over the Dataset.


``PurchaseProcedure``: Procedure
--------------------------------
This procedure has a ``history`` method to obtain the purchase history of a given customer.


Building and Running the Application and Example
================================================
In this remainder of this document, we refer to the Continuuity Reactor runtime as "Reactor", and the
example code that is running on it as an "Application".

We show the Windows prompt as ``~SDK>`` to indicate a command prompt opened in the SDK directory.

In this example, you need to build the app from source and then deploy the compiled JAR file.
You start a Continuuity Reactor, deploy the app, start the flow and then run the example by
injecting sentence entries into the stream.

Then you can start the Workflow that builds purchase histories, and after that is finished,
you can use the procedure or a SQL query to explore the results.

When finished, stop the Application as described below.

Building the Purchase Application
----------------------------------
From the project root, build ``Purchase`` with the
`Apache Maven <http://maven.apache.org>`__ command::

	$ mvn clean package

(If you modify the code and would like to rebuild the Application, you can
skip the tests by using the command::

	$ mvn -Dmaven.test.skip=true clean package


Deploying and Starting the Application
--------------------------------------
Make sure an instance of the Continuuity Reactor is running and available.
From within the SDK root directory, this command will start Reactor in local mode::

	$ ./bin/reactor.sh start

On Windows::

	~SDK> bin\reactor.bat start

From within the Continuuity Reactor Dashboard (`http://localhost:9999/ <http://localhost:9999/>`__ in local mode):

#. Drag and drop the Application .JAR file (``target/Purchase-<version>.jar``)
   onto your browser window.
   Alternatively, use the *Load App* button found on the *Overview* of the Reactor Dashboard.
#. Once loaded, select the ``Purchase`` Application from the list.
   On the Application's detail page, click the *Start* button on **both** the *Process* and *Query* lists.
#. Note: the CatalogLookupService will not be displayed in the dashboard

On Windows:

#. To deploy the App JAR file, run ``~SDK> bin\app-manager.bat deploy`` or drag and drop the
   Application .JAR file (``target/Purchase-<version>.jar`` onto your browser window.
   (Currently, the *Load App* button does not work under Windows.)
#. To start the App, run ``~SDK> bin\app-manager.bat start``

Running the Example
-------------------

Injecting Sentences
............................

Run this script to inject sentences 
to the Stream named *purchaseStream* in the ``Purchase`` application::

	$ ./bin/inject-data.sh [--host <hostname>]

:Note:	``[--host <hostname>]`` is not available for a *Local Reactor*.

On Windows::

	~SDK> bin\inject-data.bat


Starting the Workflow
.....................
The easiest way to start the ``PurchaseHistoryWorkflow`` is to click on the Workflow in the Application
page of the Reactor dashboard and then click the start button. You can see the status of the Workflow and observe when it finishes.

Alternatively, you can send a ``curl`` request to the Reactor::
  
  curl -v -X POST http://localhost:10000/v2/apps/PurchaseHistory/workflows/PurchaseHistoryWorkflow/start

Querying the Results
....................
If the Procedure has not already been started, you start it either through the 
Continuuity Reactor Dashboard or via an HTTP request using the ``curl`` command::

	curl -v -X POST 'http://localhost:10000/v2/apps/PurchaseHistory/procedures/PurchaseProcedure/start'
	
There are two ways to query the *history* ObjectStore through the ``PurchaseProcedure`` procedure:

1. Send a query via an HTTP request using the ``curl`` command. For example::

	curl -v -d '{"customer": "Alice"}' \
	  -X POST 'http://localhost:10000/v2/apps/PurchaseHistory/procedures/PurchaseProcedure/methods/history'

  On Windows, a copy of ``curl`` is located in the ``libexec`` directory of the example::

	  libexec\curl...

2. Click on the ``PurchaseProcedure`` in the Application page of the Dashboard to get to the 
   Procedure dialogue. Type in the method name ``history``, and enter the customer name in the parameters
   field, such as::

	{ "customer" : "Alice" }

   Then click the *Execute* button. The purchase history for that customer will be displayed in the
   Dashboard in JSON format, for example [reformatted to fit]::

	{"customer":"Alice","purchases"
	   [{"customer":"Alice",
	      "product":"coconut","quantity":2,"price":5,"purchaseTime":1404268588338,"catalogId":""}]}

Exploring the Results Using SQL
...............................
You can use SQL to formulate ad-hoc queries over the *history* Dataset. This is done by a series of
``curl`` calls, as described in the REST API section of the Developer Guide. For your convenience, the SDK
includes a script, ``bin/send-query.sh``, that will execute a series of calls.

From within the SDK root directory::

  send-query.sh --query  "SELECT * FROM continuuity_user_history WHERE customer IN ('Alice','Bob')"

This will submit the query, wait for its completion and then retrieve and print all results, one by one::

  Query handle is ad004d63-7e8d-44f8-b53a-33f3cf3bd5c8.
  ["Alice","[{\"customer\":\"Alice\",\"product\":\"grapefruit\",\"quantity\":12,\"price\":10
    \"purchasetime\":1403737694225}]"]
  ["Bob","[{\"customer\":\"Bob\",\"product\":\"orange\",\"quantity\":6,\"price\":12
    \"purchasetime\":1403737694226}]"]
  . . .

If you prefer to use ``curl`` directly, here is the sequence of steps to execute:

First, submit the query for execution::

  curl -v -d '{"query": "'"SELECT * FROM continuuity_user_history WHERE customer IN ('Alice','Bob')"'"}'
    -X POST http://localhost:10000/v2/data/queries

Note that due to the mix and repetition of single and double quotes, it can be tricky to escape all quotes
correctly at the shell command prompt. On success, this will return a handle for the query, such as::

  {"handle":"363f8ceb-29fe-493d-810f-858ed0440782"}

This handle is needed to inquire about the status of the query and to retrieve query results. To get the
status, issue a GET to the query's URL using the handle::

  curl -v -X GET http://localhost:10000/v2/data/queries/363f8ceb-29fe-493d-810f-858ed0440782/status

Because a SQL query can run for several minutes, you may have to repeat the call until it returns a status of *finished*::

  {"status":"FINISHED","hasResults":true}

Once execution has finished, you can retrieve the results of the query using the handle::

  curl -v -X POST http://localhost:10000/v2/data/queries/363f8ceb-29fe-493d-810f-858ed0440782/next

This will return—up to a limited number of—the results in JSON format::

  [{"columns":["Alice","[{\"customer\":\"Alice\",\"product\":\"grapefruit\",\"quantity\":12,\"price\":10
    \"purchasetime\":1403737694225}]"]},
  {"columns":["Bob","[{\"customer\":\"Bob\",\"product\":\"orange\",\"quantity\":6,\"price\":12
    \"purchasetime\":1403737694226}]"]}]

You repeat this step until the ``curl`` call returns an empty list. That means you have retrieved all of the results and you can now close the query::

  curl -v -X DELETE http://localhost:10000/v2/data/queries/363f8ceb-29fe-493d-810f-858ed0440782

Stopping the Application
------------------------
Either:

- On the Application detail page of the Reactor Dashboard, click the *Stop* button on **both** the *Process* and *Query* lists; 

or:

- Run ``$ ./bin/app-manager.sh --action stop [--host <hostname>]``

  :Note:	[--host <hostname>] is not available for a *Local Reactor*.

  On Windows, run ``~SDK> bin\app-manager.bat stop``


Downloading the Example
=======================
This example (and more!) is included with our `software development kit <http://continuuity.com/download>`__.
