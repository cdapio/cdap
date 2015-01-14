.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform Purchase Application
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _examples-purchase:

========
Purchase
========

A Cask Data Application Platform (CDAP) Example demonstrating many of the CDAP components.

Overview
========

This example demonstrates use of many of the CDAP components—Streams, Flows, Flowlets,
Datasets, Queries, MapReduce Jobs, Workflows, and Services—all in a single Application.

The application uses a scheduled MapReduce Job and Workflow to read from one ObjectStore Dataset
and write to another.

  - Send sentences of the form "Tom bought 5 apples for $10" to the ``purchaseStream``.
    You can send sentences either by using a ``curl`` call, using the ``inject-data`` script
    included in the example's ``/bin`` directory, or by using the CDAP Console.
  - The ``PurchaseFlow`` reads the ``purchaseStream`` and converts every input String into a
    Purchase object and stores the object in the *purchases* Dataset.
  - User profile information for the user can be added by using ``curl`` calls (or another method) which are
    then stored in the *userProfiles* Dataset.
  - The ``CatalogLookupService`` fetches the catalog id for a given product. The CatalogLookupService
    is called from the PurchaseStore Flowlet. The host and port of the CatalogLookupService is discovered
    using the Service discovery framework.
  - The ``UserProfileService`` is responsible for storing and retrieving the user information
    for a given user id from the *userProfiles* Dataset. The host and port of the ``UserProfileService`` is
    discovered using the Service discovery framework.
  - When scheduled by the ``PurchaseHistoryWorkFlow``, the ``PurchaseHistoryBuilder`` MapReduce
    job reads the *purchases* Dataset. It fetches the user profile information, if it is available, from
    the ``UserProfileService`` and creates a purchase history. It stores the purchase history in the
    *history* Dataset every morning at 4:00 A.M.
  - You can either manually (in the Process screen of the CDAP Console) or 
    programmatically execute the ``PurchaseHistoryBuilder`` MapReduce job to store 
    customers' purchase history in the *history* Dataset.
  - Request the ``PurchaseHistoryService`` retrieve from the *history* Dataset the purchase history of a user.
  - Execute a SQL query over the *history* Dataset. You can do this using a series of ``curl``
    calls, or more conveniently using the :ref:`Command Line Interface: <cli>`.

**Note:** Because the PurchaseHistoryWorkFlow is only scheduled to run at 4:00 A.M.,
you should not start it manually until after entering the first customers' purchases, or the
PurchaseHistoryService will responds with *204 No Response* status code.

Let's look at some of these components, and then run the Application and see the results.

The Purchase Application
------------------------

As in the other `examples <index.html>`__, the components
of the Application are tied together by the class ``PurchaseApp``:

.. literalinclude:: /../../../cdap-examples/Purchase/src/main/java/co/cask/cdap/examples/purchase/PurchaseApp.java
   :language: java
   :prepend: public class PurchaseApp extends AbstractApplication {
   :start-after: public class PurchaseApp extends AbstractApplication {

``PurchaseHistory`` and ``Purchase``: ObjectStore Data Storage
------------------------------------------------------------------------

The raw purchase data is stored in an ObjectStore Dataset, *purchases*,
with this method defined in ``PurchaseStore.java``:

.. literalinclude:: /../../../cdap-examples/Purchase/src/main/java/co/cask/cdap/examples/purchase/PurchaseStore.java
   :language: java
   :start-after: @ProcessInput
   :end-before: /**

This method is what actually puts data into the *purchases* Dataset, by writing to the
Dataset with each purchase's timestamp and the ``Purchase`` Object.

The purchase history for each customer is compiled by the ``PurchaseHistoryWorkflow``, which uses a
MapReduce Job—``PurchaseHistoryBuilder``—to aggregate all purchases into a per-customer purchase
history. It writes to the *history* Dataset, a custom Dataset that embeds an ``ObjectStore`` and 
implements the ``RecordScannable`` interface to allow SQL queries over the Dataset.


``PurchaseHistoryService``: Service
------------------------------------------------

This service has a ``history/{customer}`` endpoint to obtain the purchase history of a given customer.


``UserProfileService``: Service
------------------------------------------------

This service has two endpoints:

``user`` endpoint to add a user's profile information to the system::

  ./bin/cdap-cli.sh call service PurchaseHistory.UserProfileService POST user body \
    "{'id':'alice','firstName':'Alice','lastName':'Bernard','categories':['fruits']}"

``user/{id}`` endpoint to obtain profile information for a specified user::

  ./bin/cdap-cli.sh call service PurchaseHistory.UserProfileService GET user/alice

Building and Starting
=================================

- You can either build the example (as described `below
  <#building-an-example-application>`__) or use the pre-built JAR file included in the CDAP SDK.
- Start CDAP, deploy and start the application and its components as described below in 
  `Running CDAP Applications`_\ .
  Make sure you start the Flow, Services, and Workflow as described below.
- Once the application has been deployed and started, you can `run the example. <#running-the-example>`__

Running CDAP Applications
============================================

.. include:: /../../developers-manual/source/getting-started/building-apps.rst
   :start-line: 9

Running the Example
===================

.. highlight:: console

Starting the Flow
------------------------------

Once the application is deployed:

- Click on the *Process* button in the left sidebar of the CDAP Console,
  then click *PurchaseFlow* in the *Process* page to get to the
  Flow detail page, then click the *Start* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface:

  .. list-table::
    :widths: 20 80
    :stub-columns: 1

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh start flow PurchaseHistory.PurchaseFlow``
    * - On Windows:
      - ``> bin\cdap-cli.bat start flow PurchaseHistory.PurchaseFlow``    

Starting the Services
------------------------------

Once the application is deployed:

- Click on *PurchaseHistory* in the Overview page of the CDAP Console to get to the
  Application detail page, click *PurchaseHistoryService* in the *Service* pane to get to the
  Service detail page, then click the *Start* button; do the same for the *CatalogLookupService*; or
- From the Standalone CDAP SDK directory, use the Command Line Interface:

  .. list-table::
    :widths: 20 80
    :stub-columns: 1

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh start flow PurchaseHistory.PurchaseHistoryService``
    * - 
      - ``$ ./bin/cdap-cli.sh start flow PurchaseHistory.CatalogLookupService``
    * - On Windows:
      - ``> bin\cdap-cli.bat start flow PurchaseHistory.PurchaseHistoryService``    
    * - 
      - ``> bin\cdap-cli.bat start flow PurchaseHistory.CatalogLookupService``    

- You can send ``curl`` requests to CDAP::

    curl -v -X POST 'http://localhost:10000/v2/apps/PurchaseHistory/services/PurchaseHistoryService/start'
    curl -v -X POST 'http://localhost:10000/v2/apps/PurchaseHistory/services/CatalogLookupService/start'

  **Note:** A version of ``curl`` that works with Windows is included in the CDAP Standalone
  SDK in ``libexec\bin\curl.exe``


Injecting Sentences
------------------------------

Run this script (from within ``/examples/Purchase``) to inject sentences 
to the Stream named *purchaseStream* in the ``PurchaseHistory`` application:

.. list-table::
  :widths: 20 80
  :stub-columns: 1

  * - On Linux:
    - ``$ ./bin/inject-data.sh``
  * - On Windows:
    - ``> bin\inject-data.bat``    


Starting the Workflow
------------------------------

Once the sentences have been injected:

- Click on *PurchaseHistory* in the Overview page of the CDAP Console to get to the
  Application detail page, click *PurchaseHistoryWorkflow* in the *Workflow* pane to get to the
  Workflow detail page, then click the *Start* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface:

  .. list-table::
    :widths: 20 80
    :stub-columns: 1

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh start workflow PurchaseHistory.PurchaseHistoryWorkflow``
    * - On Windows:
      - ``> bin\cdap-cli.bat start workflow PurchaseHistory.PurchaseHistoryWorkflow``    

- You can send a ``curl`` request to CDAP::

    curl -v -X POST 'http://localhost:10000/v2/apps/PurchaseHistory/workflows/PurchaseHistoryWorkflow/start'

  **Note:** A version of ``curl`` that works with Windows is included in the CDAP Standalone
  SDK in ``libexec\bin\curl.exe``

Querying the Results
------------------------------

To query the *history* ObjectStore through the ``PurchaseHistoryService``, you can

- From the Standalone CDAP SDK directory, use the Command Line Interface:

  .. list-table::
    :widths: 20 80
    :stub-columns: 1

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh call service PurchaseHistory.PurchaseHistoryService GET history/Alice``
    * - On Windows:
      - ``> bin\cdap-cli.bat call service PurchaseHistory.PurchaseHistoryService GET history/Alice``

- Send a query via an HTTP request using the ``curl`` command. For example::

    curl -w '\n' -v \
      'http://localhost:10000/v2/apps/PurchaseHistory/services/PurchaseHistoryService/methods/history/Alice'

**Note:** A version of ``curl`` that works with Windows is included in the CDAP Standalone
SDK in ``libexec\bin\curl.exe``
  
Exploring the Results Using SQL
-------------------------------

You can use SQL to formulate ad-hoc queries over the *history* Dataset. This is done by a series of
``curl`` calls, as described in the :ref:`RESTful API <http-restful-api-query>` section of the 
:ref:`CDAP Reference Manual. <reference-index>`
For your convenience, the SDK includes a script, ``bin/cdap-cli.sh``, that can execute the series of calls.

From within the SDK root directory::

  ./bin/cdap-cli.sh execute "\"SELECT * FROM cdap_user_history WHERE customer IN ('Alice','Bob')\""
  
(On Windows, use ``bin\cdap-cli.bat ...``)

This will submit the query, using the *history* table in the ``cdap_user`` namespace, wait
for its completion and then retrieve and print all results, one by one (example
reformatted to fit)::


  +========================================================================================+
  | cdap_user_history.customer: STRING | cdap_user_history.purchases:                      |
  |                                    |   array<struct<customer:string,                   |
  |                                    |                userProfile:<struct<               |
  |                                    |                          id:string,               |
  |                                    |                          firstName:string,        |
  |                                    |                          lastName:string,         |
  |                                    |                          categories:array<string> |
  |                                    |                          >> optional,             |
  |                                    |                product:string,                    |
  |                                    |                quantity:int,                      |
  |                                    |                price:int,                         |
  |                                    |                purchasetime:bigint,               |
  |                                    |                catalogid:string>>                 |
  +========================================================================================+
  | Alice                              | [{"customer":"Alice",                             |
  |                                    |                "userProfile":{                    |
  |                                    |                    "id":"alice",                  |
  |                                    |                    "firstName":"Alice",           |
  |                                    |                    "lastName":"Bernard",          |
  |                                    |                    "categories":[                 |
  |                                    |                          "fruits"                 |
  |                                    |                          ]                        |
  |                                    |                },                                 |
  |                                    |                "product":"coconut",               |
  |                                    |                "quantity":2,                      |
  |                                    |                "price":5,                         |
  |                                    |                "purchasetime":1415237567039,      |
  |                                    |                "catalogid":"Catalog-coconut"},    |
  |                                    |  {"customer":"Alice",                             |
  |                                    |                "userProfile":{                    |
  |                                    |                    "id":"alice",                  |
  |                                    |                    "firstName":"Alice",           |
  |                                    |                    "lastName":"Bernard",          |
  |                                    |                    "categories":[                 |
  |                                    |                          "fruits"                 |
  |                                    |                          ]                        |
  |                                    |                },                                 |
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

  curl -w '\n' -v -d '{"query": "'"SELECT * FROM cdap_user_history WHERE customer IN ('Alice','Bob')"'"}' \
    http://localhost:10000/v2/data/explore/queries;

Note that due to the mix and repetition of single and double quotes, it can be tricky to escape all quotes
correctly at the shell command prompt. On success, this will return a handle for the query, such as::

  {"handle":"363f8ceb-29fe-493d-810f-858ed0440782"}

This handle is needed to inquire about the status of the query and to retrieve query results. To get the
status, issue a GET to the query's URL using the handle::

  curl -w '\n' -v -X GET http://localhost:10000/v2/data/explore/queries/363f8ceb-29fe-493d-810f-858ed0440782/status

Because a SQL query can run for several minutes, you may have to repeat the call until it returns a status of *finished*::

  {"status":"FINISHED","hasResults":true}

Once execution has finished, you can retrieve the results of the query using the handle::

  curl -w '\n' -v -X POST http://localhost:10000/v2/data/explore/queries/363f8ceb-29fe-493d-810f-858ed0440782/next

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
Once done, you can stop the application as described above in `Stopping an Application. 
<#stopping-an-application>`__ Here is an example-specific description of the steps:

**Stopping the Flow**

- Click on the *Process* button in the left sidebar of the CDAP Console,
  then click *PurchaseFlow* in the *Process* page to get to the
  Flow detail page, then click the *Stop* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface:

  .. list-table::
    :widths: 20 80
    :stub-columns: 1

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh stop flow PurchaseHistory.PurchaseFlow``
    * - On Windows:
      - ``> bin\cdap-cli.bat stop flow PurchaseHistory.PurchaseFlow``    

**Stopping the Services**

- Click on *PurchaseHistory* in the Overview page of the CDAP Console to get to the
  Application detail page, click *PurchaseHistoryService* in the *Service* pane to get to the
  Service detail page, then click the *Stop* button; do the same for *CatalogLookupService*; or
- From the Standalone CDAP SDK directory, use the Command Line Interface:

  .. list-table::
    :widths: 20 80
    :stub-columns: 1

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh start flow PurchaseHistory.PurchaseHistoryService``
    * - 
      - ``$ ./bin/cdap-cli.sh start flow PurchaseHistory.CatalogLookupService``
    * - On Windows:
      - ``> bin\cdap-cli.bat start flow PurchaseHistory.PurchaseHistoryService``    
    * - 
      - ``> bin\cdap-cli.bat start flow PurchaseHistory.CatalogLookupService``    
