.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform Purchase Application
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _examples-purchase:

========
Purchase
========

A Cask Data Application Platform (CDAP) Example demonstrating many of the CDAP elements.

Overview
========

This example demonstrates use of many of the CDAP elements—Streams, Flows, Flowlets,
Datasets, Queries, Procedures, MapReduce Jobs, Workflows, and Custom Services—all in a single Application.

The application uses a scheduled MapReduce Job and Workflow to read from one ObjectStore Dataset
and write to another.

  - Send sentences of the form "Tom bought 5 apples for $10" to the ``purchaseStream``.
    You can send sentences either by using a ``curl`` call, using the ``inject-data`` script
    included in the example's ``/bin`` directory, or using the CDAP Console.
  - The ``PurchaseFlow`` reads the ``purchaseStream`` and converts every input String into a
    Purchase object and stores the object in the *purchases* Dataset.
  - The ``CatalogLookupService`` fetches the catalog id for a given product. The CatalogLookupService
    is called from the PurchaseStore Flowlet. The host and port of the CatalogLookupService is discovered
    using the Service discovery framework.
  - When scheduled by the ``PurchaseHistoryWorkFlow``, the ``PurchaseHistoryBuilder`` MapReduce
    job reads the *purchases* Dataset, creates a purchase history, and stores the purchase
    history in the *history* Dataset every morning at 4:00 A.M.
  - You can either manually (in the Process screen of the CDAP Console) or 
    programmatically execute the ``PurchaseHistoryBuilder`` MapReduce job to store 
    customers' purchase history in the *history* Dataset.
  - Execute the ``PurchaseProcedure`` procedure to query the *history* Dataset to discover the
    purchase history of each user.
  - Execute a SQL query over the *history* Dataset. You can do this using a series of ``curl``
    calls, or more conveniently using the ``cdap-cli``.

**Note:** Because the PurchaseHistoryWorkFlow is only scheduled to run at 4:00 A.M.,
you should not start it manually until after entering the first customers' purchases, or the
PurchaseProcedure will return a "not found" error.

Let's look at some of these elements, and then run the Application and see the results.

The Purchase Application
------------------------

As in the other `examples <index.html>`__, the components
of the Application are tied together by the class ``PurchaseApp``:

.. literalinclude:: /../../../cdap-examples/Purchase/src/main/java/co/cask/cdap/examples/purchase/PurchaseApp.java
   :language: java
   :lines: 29-

``PurchaseHistory`` and ``Purchase``: ObjectStore Data Storage
------------------------------------------------------------------------

The raw purchase data is stored in an ObjectStore Dataset, *purchases*,
with this method defined in ``PurchaseStore.java``:

.. literalinclude:: /../../../cdap-examples/Purchase/src/main/java/co/cask/cdap/examples/purchase/PurchaseStore.java
   :language: java
   :lines: 44

This method is what actually puts data into the *purchases* Dataset, by writing to the
Dataset with each purchase's timestamp and the ``Purchase`` Object.

The purchase history for each customer is compiled by the ``PurchaseHistoryWorkflow``, which uses a
MapReduce Job—``PurchaseHistoryBuilder``—to aggregate all purchases into a per-customer purchase
history. It writes to the *history* Dataset, a custom Dataset that embeds an ``ObjectStore`` and 
implements the ``RecordScannable`` interface to allow SQL queries over the Dataset.


``PurchaseProcedure``: Procedure
------------------------------------------------

This procedure has a ``history`` method to obtain the purchase history of a given customer.


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

.. highlight:: console

Injecting Sentences
------------------------------

Run this script to inject sentences 
to the Stream named *purchaseStream* in the ``Purchase`` application::

  $ ./bin/inject-data.sh

On Windows::

  > bin\inject-data.bat


Starting the Workflow
------------------------------

The easiest way to start the ``PurchaseHistoryWorkflow`` is to click on the Workflow in
the Application page of the CDAP Console and then click the *start* button. You can see
the status of the Workflow and observe when it finishes.

Alternatively, you can send a ``curl`` request to CDAP::
  
  curl -v -X POST 'http://localhost:10000/v2/apps/PurchaseHistory/workflows/PurchaseHistoryWorkflow/start'

Querying the Results
------------------------------

If the Procedure has not already been started, you start it either through the 
CDAP Console or via an HTTP request using the ``curl`` command::

  curl -v -X POST 'http://localhost:10000/v2/apps/PurchaseHistory/procedures/PurchaseProcedure/start'
  
There are two ways to query the *history* ObjectStore through the ``PurchaseProcedure`` procedure:

1. Send a query via an HTTP request using the ``curl`` command. For example::

    curl -v -d '{"customer": "Alice"}' \
      'http://localhost:10000/v2/apps/PurchaseHistory/procedures/PurchaseProcedure/methods/history'; echo

   On Windows, a copy of ``curl`` is located in the ``libexec`` directory of the example::

    libexec\curl...

2. Click on the ``PurchaseProcedure`` in the Application's detail page of the Console to get to the 
   Procedure dialogue. Type in the method name ``history``, and enter the customer name in the parameters
   field, such as::

    { "customer" : "Alice" }

   Then click the *Execute* button. The purchase history for that customer will be displayed in the
   Console in JSON format (example reformatted to fit)::

    {"customer":"Alice","purchases":
      [{"customer":"Alice",
        "product":"coconut","quantity":2,"price":5,"purchaseTime":1414993175135,"catalogId":""},
       {"customer":"Alice",
        "product":"grapefruit","quantity":12,"price":10,"purchaseTime":1414993175134,"catalogId":""}]}


Exploring the Results Using SQL
-------------------------------

You can use SQL to formulate ad-hoc queries over the *history* Dataset. This is done by a series of
``curl`` calls, as described in the :ref:`RESTful API <http-restful-api-query>` section of the 
:ref:`CDAP Reference Manual. <reference-index>`
For your convenience, the SDK includes a script, ``bin/cdap-cli.sh``, that can execute the series of calls.

From within the SDK root directory::

  ./bin/cdap-cli.sh execute "SELECT * FROM cdap_user_history WHERE customer IN ('Alice','Bob')"

This will submit the query, using the *history* table in the ``cdap_user`` namespace, wait
for its completion and then retrieve and print all results, one by one (example
reformatted to fit)::


  +========================================================================================+
  | cdap_user_history.customer: STRING | cdap_user_history.purchases:                      |
  |                                    |   array<struct<customer:string,                   |
  |                                    |                product:string,                    |
  |                                    |                quantity:int,                      |
  |                                    |                price:int,                         |
  |                                    |                purchasetime:bigint,               |
  |                                    |                catalogid:string>>                 |
  +========================================================================================+
  | Alice                              | [{"customer":"Alice",                             |
  |                                    |                "product":"coconut",               |
  |                                    |                "quantity":2,                      |
  |                                    |                "price":5,                         |
  |                                    |                "purchasetime":1415237567039,      |
  |                                    |                "catalogid":"Catalog-coconut"},    |
  |                                    |  {"customer":"Alice",                             |
  |                                    |                "product":"grapefruit",            |
  |                                    |                "quantity":12,                     |
  |                                    |                "price":10,                        |
  |                                    |                "purchasetime":1415237567016,      |
  |                                    |                "catalogid":"Catalog-grapefruit"}] |
  | Bob                                | [{"customer":"Bob",                               |
  |                                    |                "product":"coffee",                |
  |                                    |                "quantity":1,                      |
  |                                    |                "price":1,                         | 
  |                                    |                "purchasetime":1415237567056,      |
  |                                    |                "catalogid":"Catalog-coffee"},     |
  |                                    |  {"customer":"Bob",                               |
  |                                    |                "product":"orange",                |
  |                                    |                "quantity":6,                      |
  |                                    |                "price":12,                        |  
  |                                    |                "purchasetime":1415237567025,      |
  |                                    |                "catalogid":"Catalog-orange"}]     |
  | . . .                              |  . . .                                            |
  +========================================================================================+

If you prefer to use ``curl`` directly, here is the sequence of steps to execute:

First, submit the query for execution::

  curl -v -d '{"query": "'"SELECT * FROM cdap_user_history WHERE customer IN ('Alice','Bob')"'"}' \
    http://localhost:10000/v2/data/explore/queries; echo

Note that due to the mix and repetition of single and double quotes, it can be tricky to escape all quotes
correctly at the shell command prompt. On success, this will return a handle for the query, such as::

  {"handle":"363f8ceb-29fe-493d-810f-858ed0440782"}

This handle is needed to inquire about the status of the query and to retrieve query results. To get the
status, issue a GET to the query's URL using the handle::

  curl -v -X GET http://localhost:10000/v2/data/explore/queries/363f8ceb-29fe-493d-810f-858ed0440782/status; echo

Because a SQL query can run for several minutes, you may have to repeat the call until it returns a status of *finished*::

  {"status":"FINISHED","hasResults":true}

Once execution has finished, you can retrieve the results of the query using the handle::

  curl -v -X POST http://localhost:10000/v2/data/explore/queries/363f8ceb-29fe-493d-810f-858ed0440782/next; echo

This will return—up to a limited number—the results in JSON format::

  [{"columns":["Alice","[{\"customer\":\"Alice\",\"product\":\"grapefruit\",\"quantity\":12,\"price\":10
    \"purchasetime\":1403737694225}]"]},
  {"columns":["Bob","[{\"customer\":\"Bob\",\"product\":\"orange\",\"quantity\":6,\"price\":12
    \"purchasetime\":1403737694226}]"]}]
  . . .

You repeat this step until the ``curl`` call returns an empty list. That means you have
retrieved all of the results and you can now close the query::

  curl -v -X DELETE http://localhost:10000/v2/data/explore/queries/363f8ceb-29fe-493d-810f-858ed0440782

Stopping the Application
-------------------------------
Once done, you can stop the application as described above in `Stopping an Application. <#stopping-an-application>`__
