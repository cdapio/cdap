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
This example demonstrates use of all the Reactor elements: Streams, Flow, Flowlets,
Datasets, Queries, Procedures, MapReduce jobs, and Workflows in a single Application.

The application uses a scheduled MapReduce job and Workflows to read from one ObjectStore dataset
and write to another.

  - Send sentences of the form "Tom bought 5 apples for $10" to the ``purchaseStream``.
    You can send sentences either by using a ``curl`` call, using the ``inject-data`` script
    included in the example's ``/bin`` directory, or using the Continuuity Reactor Dashboard.
  - The ``PurchaseFlow`` reads the ``purchaseStream`` and converts every input String into a
    Purchase object and stores the object in the *purchases* dataset.
  - When scheduled by the ``PurchaseHistoryWorkFlow``, the ``PurchaseHistoryBuilder`` MapReduce
    job reads the *purchases* dataset, creates a purchase history, and stores the purchase
    history in the *history* dataset every morning at 4:00 A.M.
  - You can either manually (in the Process screen of the Reactor Dashboard) or 
    programmatically execute the ``PurchaseHistoryBuilder`` MapReduce job to store 
    customers' purchase history in the *history* dataset.
  - Execute the ``PurchaseProcedure`` procedure to query the *history* dataset to discover the
    purchase history of each user.
  - Execute a SQL query over the *history* dataset. You can do this using a series of ``curl``
    calls, or more conveniently using the ``send-query`` script.

**Note:** Because the PurchaseHistoryWorkFlow is only scheduled to run at 4:00 A.M.,
you should or start it manually after entering the first customers' purchases, or the
PurchaseProcedure will return a "not found" error.

Let's look at some of these elements, and then run the Application and see the results.

The Purchase Application
------------------------
As in the other `examples <http://continuuity.com/developers/examples>`__, the components
of the Application are tied together by the class ``PurchaseApp``::

  public class PurchaseApp extends AbstractApplication {

    @Override
    public void configure() {
      setName("PurchaseHistory");
      setDescription("Purchase history app");
      addStream(new Stream("purchaseStream"));
      createDataSet("frequentCustomers", KeyValueTable.class);
      addFlow(new PurchaseFlow());
      addProcedure(new PurchaseProcedure());
      addWorkflow(new PurchaseHistoryWorkflow());

      try {
        createDataSet("history", PurchaseHistoryStore.class, PurchaseHistoryStore.properties());
        ObjectStores.createObjectStore(getConfigurer(), "purchases", Purchase.class);
      } catch (UnsupportedTypeException e) {
        // this exception is thrown by ObjectStore if its parameter type cannot be (de)serialized (for example, if it is
        // an interface and not a class, then there is no auto-magic way deserialize an object. In this case that
        // cannot happen because PurchaseHistory is an actual class.
        throw new RuntimeException(e);
      }
    }
  }


``PurchaseHistory`` and ``Purchase``: ObjectStore Data Storage
--------------------------------------------------------------
The raw purchase data is stored in an ObjectStore dataset, ``Purchase``,
with this method defined in ``PurchaseStore``::

  ``process(Purchase purchase)``

This method is what actually puts data into the ``Purchase`` dataset, by writing to the
dataset with each purchase's timestamp and the purchase Object.

The purchase history for each customer is compiled by the ``PurchaseHistoryWorkflow``, which uses a Map/Reduce job,
``PurchaseHistoryBuilder``, to aggregate all purchases into per-customer purchase history. It writes to the *history*
dataset, a custom dataset that embeds an ``ObjectStore`` and also implements the ``RecordScannable`` interface to
allow SQL queries over this dataset.


``PurchaseProcedure``: Procedure
--------------------------------
This procedure has a ``history`` method to obtain the purchase history of one given customer.


Building and Running the Application and Example
================================================
In this remainder of this document, we refer to the Continuuity Reactor runtime as "Reactor", and the
example code that is running on it as an "Application".

We show the Windows prompt as ``~SDK>`` to indicate a command prompt opened in the SDK directory.

In this example, you need to build the app from source and then deploy the compiled JAR file.
You start a Continuuity Reactor, deploy the app, start the flow and then run the example by
injecting sentence entries into the stream.

Then you can start the workflow that builds purchase histories, and after that is finished,
you can use the procedure or a SQL query to explore the results.

When finished, stop the Application as described below.

Building Purchase Application
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

	~SDK> bin\reactor start

From within the Continuuity Reactor Dashboard (`http://localhost:9999/ <http://localhost:9999/>`__ in local mode):

#. Drag and drop the Application .JAR file (``target/Purchase-2.3.0.jar``)
   onto your browser window.
   Alternatively, use the *Load App* button found on the *Overview* of the Reactor Dashboard.
#. Once loaded, select the ``Purchase`` Application from the list.
   On the Application's detail page, click the *Start* button on **both** the *Process* and *Query* lists.

On Windows:

#. To deploy the App JAR file, run ``~SDK> bin\appManager deploy``
#. To start the App, run ``~SDK> bin\appManager start``

Running the Example
-------------------

Injecting Sentences
............................

Run this script to inject sentences 
to the Stream named *purchaseStream* in the ``Purchase`` application::

	$ ./bin/inject-data.sh [--gateway <hostname>]

:Note:	[--gateway <hostname>] is not available for a *Local Reactor*.

On Windows::

	~SDK> bin\inject-data


Starting the Workflow
.....................
The easiest way to start the ``PurchaseHistoryWorkflow`` is to click on the workflow in the application page of the
Reactor dashboard and then click the start button. You can then also see the status of the workflow and when it
finishes.

Alternatively, you can send a ``curl`` request to the Reactor::

  curl -v -X POST http://localhost:10000/v2/apps/Purchase/procedures/PurchaseQuery/start

Querying the Results
....................
There are two ways to query the *history* ObjectStore through the ``PurchaseProcedure`` procedure:

- Send a query via an HTTP request using the ``curl`` command. For example::

	  curl -v -d '{"customer": "Alice"}' -X POST 'http://localhost:10000/v2/apps/Purchase/procedures/PurchaseProcedure/methods/history'

  On Windows, a copy of ``curl`` is located in the ``libexec`` directory of the example::

	  libexec\curl...

- Click on the ``PurchaseProcedure`` in the application page of the dashboard to get to the procedure dialogue. Type
  the method name ``history``, and enter the customer name in the parameters field, for example::

    { "customer" : "Alice" }

	Then click the *Execute* button. The purchase history for that customer will be displayed in the
	Dashboard in JSON format, for example::

    {"customer":"Alice","purchases":[{"customer":"Alice","product":"grapefruit","quantity":12,"price":10,"purchaseTime":1403737694225}]}

Exploring the results using SQL
...............................
You can use SQL to formulate ad-hoc queries over the *history* dataset. This is done by a series of ``curl`` calls, as
described in the REST API section of the Developer Guide. For your convenience, this example includes a script,
``send-query`` to execute this series of calls::

  send-query.sh --query  "SELECT * FROM continuuity_user_history WHERE customer IN ('Alice','Bob')"

This will submit the query, wait for its completion and then retrieve and print all results one by one::

  Query handle is ad004d63-7e8d-44f8-b53a-33f3cf3bd5c8.
  ["Alice","[{\"customer\":\"Alice\",\"product\":\"grapefrui\",\"quantity\":12,\"price\":10,\"purchasetime\":1403737694225}]"]
  ["Bob","[{\"customer\":\"Bob\",\"product\":\"orange\",\"quantity\":6,\"price\":12,\"purchasetime\":1403737694226}]"]

If you prefer to use ``curl`` directly, here is the sequence of steps to execute:

The first call is to submit the query for execution::

  curl -v -d '{"query": "'"SELECT * FROM continuuity_user_history WHERE customer IN ('Alice','Bob')"'"}' -X POST http://localhost:10000/v2/data/queries

Note that due to the mix and repetition of single and double quotes, it can be tricky to escape all quotes correctly
at the shell command prompt. On success, this will return a handle for the query::

  {"handle":"363f8ceb-29fe-493d-810f-858ed0440782"}

This handle is needed to inquire about the status of the query and to retrieve query results. To get the status,
issue a GET to the query's URL::

  curl -v -X GET http://localhost:10000/v2/data/queries/363f8ceb-29fe-493d-810f-858ed0440782/status

Because a SQL query can run for several minutes, you may have to repeat this call until it returns a status of finished:

  {"status":"FINISHED","hasResults":true}

Now that the execution is finished, you can retrieve the results of the query::

  curl -v -X POST http://localhost:10000/v2/data/queries/363f8ceb-29fe-493d-810f-858ed0440782/next

This will return up to a limited number of results in JSON format, for example::

  [{"columns":["Alice","[{\"customer\":\"Alice\",\"product\":\"grapefruit\",\"quantity\":12,\"price\":10,\"purchasetime\":1403737694225}]"]},{"columns":["Bob","[{\"customer\":\"Bob\",\"product\":\"orange\",\"quantity\":6,\"price\":12,\"purchasetime\":1403737694226}]"]}]

You can repeat this step until the ``curl`` call returns an empty list. That means you have rerieved all results and
you can now close the query::

  curl -v -X DELETE http://localhost:10000/v2/data/queries/363f8ceb-29fe-493d-810f-858ed0440782

Stopping the Application
------------------------
Either:

- On the Application detail page of the Reactor Dashboard, click the *Stop* button on **both** the *Process* and *Query* lists; or
- Run ``$ ./bin/appManager.sh --action stop [--gateway <hostname>]``

  :Note:	[--gateway <hostname>] is not available for a *Local Reactor*.

  On Windows, run ``~SDK> bin\appManager stop``

