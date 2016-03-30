.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform Purchase Application
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _examples-purchase:

========
Purchase
========

A Cask Data Application Platform (CDAP) example demonstrating many of the CDAP components.

.. highlight:: console

Overview
========
This example demonstrates use of many of the CDAP components |---| streams, flows, flowlets,
datasets, queries, MapReduce programs, workflows, and services |---| all in a single application.

The application uses a scheduled MapReduce and workflow to read from an ``ObjectMappedTable`` dataset
and write to an ``ObjectStore`` dataset.

  - Send sentences of the form "Tom bought 5 apples for $10" to the *purchaseStream*.
    You can send sentences either by using a ``curl`` call, the CDAP CLI, or the CDAP UI.
  - The *PurchaseFlow* reads the *purchaseStream* and converts every input string into a
    Purchase object and stores the object in the *purchases* dataset.
  - The *PurchaseStore* flowlet demonstrates the setting of memory used by its YARN container. 
  - User profile information for the user can be added by using ``curl`` calls (or another method) which are
    then stored in the *userProfiles* dataset.
  - The *CatalogLookup* service fetches the catalog id for a given product. The *CatalogLookup* service
    is called from the *PurchaseStore* flowlet. The host and port of the *CatalogLookup* service is discovered
    using the service discovery framework.
  - The *UserProfileService* is responsible for storing and retrieving the user information
    for a given user ID from the *userProfiles* dataset. The host and port of the *UserProfileService* is
    discovered using the service discovery framework.
  - When scheduled by the *PurchaseHistoryWorkFlow*, the ``PurchaseHistoryBuilder`` MapReduce
    reads the *purchases* dataset. It fetches the user profile information, if it is available, from
    the *UserProfileService* and creates a purchase history. It stores the purchase history in the
    history dataset every morning at 4:00 A.M. using a time schedule, and also every time 1MB of data
    is ingested by the *purchaseStream* using a data schedule.
  - You can either manually (in the Process screen of the CDAP UI) or programmatically execute the 
    ``PurchaseHistoryBuilder`` MapReduce to store customers' purchase history in the history dataset.
  - The ``PurchaseHistoryBuilder`` MapReduce demonstrates the setting of memory used by its YARN container, both 
    as default values and as runtime arguments.
  - Use the *PurchaseHistoryService* to retrieve from the history dataset the purchase history of a user.
  - Execute a SQL query over the history dataset. You can do this using a series of curl
    calls, or more conveniently using the :ref:`Command Line Interface <cli>`.

**Note:** Because the *PurchaseHistoryWorkFlow* is only scheduled to run at 4:00 A.M.,
you should not start it manually until after entering the first customers' purchases, or the
*PurchaseHistoryService* will respond with a ``204 No Response`` status code.

Let's look at some of these components, and then run the application and see the results.

The *Purchase* Application
--------------------------
As in the other `examples <index.html>`__, the components
of the application are tied together by the class ``PurchaseApp``:

.. literalinclude:: /../../../cdap-examples/Purchase/src/main/java/co/cask/cdap/examples/purchase/PurchaseApp.java
    :language: java
    :lines: 31-

Storing Purchases with the *Purchase* ObjectStore Data Storage
--------------------------------------------------------------
The raw purchase data is stored in an ``ObjectMappedTable`` dataset, *purchases*,
with this method defined in ``PurchaseStore.java``:

.. literalinclude:: /../../../cdap-examples/Purchase/src/main/java/co/cask/cdap/examples/purchase/PurchaseStore.java
    :language: java
    :start-after: @ProcessInput
    :end-before: @Override
    :dedent: 2

This method is what actually puts data into the *purchases* dataset, by writing to the
dataset with each purchase's timestamp and the ``Purchase`` Object.

The purchase history for each customer is compiled by the *PurchaseHistoryWorkflow*, which uses a
MapReduce |---| ``PurchaseHistoryBuilder`` |---| to aggregate all purchases into a per-customer purchase
history. It writes to the *history* dataset, a custom dataset that embeds an ``ObjectStore`` and 
implements the ``RecordScannable`` interface to allow SQL queries over the dataset.

The memory requirements of the flowlet *PurchaseStore* are set in its ``configure`` method:

.. literalinclude:: /../../../cdap-examples/Purchase/src/main/java/co/cask/cdap/examples/purchase/PurchaseStore.java
   :language: java
   :lines: 60-71
   :start-after: }
   :end-before:   /**
   :dedent: 2

*PurchaseHistoryBuilder* MapReduce
----------------------------------
This MapReduce program demonstrates the setting of the YARN container resources, both as
default values used in configuration and as runtime arguments:

.. literalinclude:: /../../../cdap-examples/Purchase/src/main/java/co/cask/cdap/examples/purchase/PurchaseHistoryBuilder.java
   :language: java
   :lines: 47-77

*PurchaseHistoryService* Service
--------------------------------
This service has a ``history/{customer}`` endpoint to obtain the purchase history of a given customer. It also demonstrates
the use of ``Resources`` to configure the memory requirements of the service:

.. literalinclude:: /../../../cdap-examples/Purchase/src/main/java/co/cask/cdap/examples/purchase/PurchaseHistoryService.java
    :language: java
    :lines: 39-46
    :dedent: 2

*UserProfileService* Service
----------------------------
.. highlight:: console

This service has two endpoints:

A ``user`` endpoint to add a user's profile information to the system::

  $ cdap-cli.sh call service PurchaseHistory.UserProfileService POST user body \
    "{'id':'Alice','firstName':'Alice','lastName':'Bernard','categories':['fruits']}"

A ``user/{id}`` endpoint to obtain profile information for a specified user::

  $ cdap-cli.sh call service PurchaseHistory.UserProfileService GET user/Alice
  < 200 OK
  < Content-Length: 79
  < Connection: keep-alive
  < Content-Type: application/json
  {"id":"Alice","firstName":"Alice","lastName":"Bernard","categories":["fruits"]}

.. Building and Starting
.. =====================
.. |example| replace:: PurchaseHistory
.. |example-italic| replace:: *PurchaseHistory*
.. |example-dir| replace:: Purchase
.. |example-artifact| replace:: Purchase
.. |application-overview-page| replace:: :cdap-ui-apps-programs:`application overview page, programs tab <PurchaseHistory>`

.. include:: _includes/_building-starting-running-example-with-dir.txt


Running the Example
===================

.. Starting the Flow
.. -----------------
.. |example-flow| replace:: PurchaseFlow
.. |example-flow-italic| replace:: *PurchaseFlow*

.. include:: _includes/_starting-flow.txt

.. Starting the Services
.. ---------------------
.. |example-service1| replace:: PurchaseHistoryService
.. |example-service1-italic| replace:: *PurchaseHistoryService*

.. |example-service2| replace:: CatalogLookup
.. |example-service2-italic| replace:: *CatalogLookup*

.. |example-service3| replace:: UserProfileService
.. |example-service3-italic| replace:: *UserProfileService*

.. include:: _includes/_starting-services.txt

Add A Profile
-------------
Add a *User Profile* for the user *Alice*, by running this command from the Standalone
CDAP SDK directory, using the Command Line Interface::

  $ cdap-cli.sh call service PurchaseHistory.UserProfileService POST user body \
    "{'id':'Alice','firstName':'Alice','lastName':'Bernard','categories':['fruits']}"
  Successfully connected to CDAP instance at http://localhost:10000/default
  < 200 OK

Injecting Sentences
-------------------
Inject a file of sentences by running this command from the Standalone
CDAP SDK directory, using the Command Line Interface::

  $ cdap-cli.sh load stream purchaseStream examples/Purchase/resources/purchases.txt 
  Successfully sent stream event to stream 'purchaseStream'

.. Starting the Workflow
.. ---------------------
.. |example-workflow| replace:: PurchaseHistoryWorkflow
.. |example-workflow-italic| replace:: *PurchaseHistoryWorkflow*

.. include:: _includes/_starting-workflow.txt

Querying the Results
--------------------
To query the *history* ``ObjectStore`` through the |example-service1-italic|, you can:

- Using the CDAP-UI, go to the |application-overview|,
  click |example-service1-italic| to get to the service detail page, then click the *Start* button; or

- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh call service PurchaseHistory.PurchaseHistoryService GET history/Alice

- Or, send a query via an HTTP request using the ``curl`` command::

    $ curl -w'\n' -v 'http://localhost:10000/v3/namespaces/default/apps/PurchaseHistory/services/PurchaseHistoryService/methods/history/Alice'

The results will be in JSON::

  {"customer":"Alice","userProfile":{"id":"Alice","firstName":"Alice","lastName":"Bernard","
  categories":["fruits"]},"purchases":[{"customer":"Alice","product":"coconut","quantity":2,
  "price":5,"purchaseTime":1444411198435,"catalogId":""},{"customer":"Alice","product":"
  grapefruit","quantity":12,"price":10,"purchaseTime":1444411198432,"catalogId":""}]}

Exploring the Results using SQL
-------------------------------
You can use SQL to formulate ad-hoc queries over the *history* and *purchases* datasets.
This is done by a series of ``curl`` calls, as described in the :ref:`RESTful API
<http-restful-api-query>` section of the :ref:`CDAP Reference Manual <reference-index>`.
For your convenience, the SDK's Command Line Interface can execute the series of calls.

From within the SDK root directory::

  $ cdap-cli.sh execute "\"SELECT * FROM dataset_history WHERE customer IN ('Alice','Bob')\""

This will submit the query, using the *history* table in the ``cdap_user`` namespace, wait
for its completion and then retrieve and print all results, one by one (example
reformatted to fit)::

  +===========================================================================================================================+
  | dataset_history.customer: | dataset_history.userprofile: struct<id:string | dataset_history.purchases: array<struct<custo |
  | STRING                    | ,firstname:string,lastname:string,categories: | mer:string,product:string,quantity:int,price: |
  |                           | array<string>>                                | int,purchasetime:bigint,catalogid:string>>    |
  +===========================================================================================================================+
  | Alice                     | {"id":"Alice","firstname":"Alice","lastname": | [{"customer":"Alice","product":"coconut","qua |
  |                           | "Bernard","categories":["fruits"]}            | ntity":2,"price":5,"purchasetime":14267198729 |
  |                           |                                               | 86,"catalogid":"Catalog-coconut"},{"customer" |
  |                           |                                               | :"Alice","product":"grapefruit","quantity":12 |
  |                           |                                               | ,"price":10,"purchasetime":1426719872968,"cat |
  |                           |                                               | alogid":"Catalog-grapefruit"}]                |
  |---------------------------------------------------------------------------------------------------------------------------|
  | Bob                       |                                               | [{"customer":"Bob","product":"coffee","quanti |
  |                           |                                               | ty":1,"price":1,"purchasetime":1426719873005, |
  |                           |                                               | "catalogid":"Catalog-coffee"},{"customer":"Bo |
  |                           |                                               | b","product":"orange","quantity":6,"price":12 |
  |                           |                                               | ,"purchasetime":1426719872970,"catalogid":"Ca |
  |                           |                                               | talog-orange"}]                               |
  +===========================================================================================================================+

Note that because we only submitted a single User Profile, only one result |---| for
*Alice* |---| is returned.

Explore the Results Using curl and SQL
......................................
If you prefer to use ``curl`` directly, here are the sequence of steps to execute:

First, submit the query for execution::

  $ curl -w'\n' -v http://localhost:10000/v3/namespaces/default/data/explore/queries \
    -d '{"query": "'"SELECT * FROM dataset_history WHERE customer IN ('Alice','Bob')"'"}'

Note that due to the mix and repetition of single and double quotes, it can be tricky to escape all quotes
correctly at the shell command prompt. On success, this will return a handle for the query, such as::

  {"handle":"07fd9b6a-95b3-4831-992c-7164f11c3754"}

This handle is needed to inquire about the status of the query and to retrieve query results. To get the
status, issue a GET to the query's URL using the handle::

  $ curl -w'\n' -v -X GET http://localhost:10000/v3/data/explore/queries/07fd9b6a-95b3-4831-992c-7164f11c3754/status

Because a SQL query can run for several minutes, you may have to repeat the call until it returns a status of *finished*::

  {"status":"FINISHED","hasResults":true}

Once execution has finished, you can retrieve the results of the query using the handle::

  $ curl -w'\n' -v -X POST http://localhost:10000/v3/data/explore/queries/07fd9b6a-95b3-4831-992c-7164f11c3754/next

This will return |---| up to a limited number |---| the results in JSON format::

  [{"columns":["Alice","[{\"customer\":\"Alice\",\"product\":\"grapefruit\",\"quantity\":12,\"price\":10
    \"purchasetime\":1403737694225}]"]},
  {"columns":["Bob","[{\"customer\":\"Bob\",\"product\":\"orange\",\"quantity\":6,\"price\":12
    \"purchasetime\":1403737694226}]"]}]
  . . .

You repeat this step until the ``curl`` call returns an empty list. That means you have
retrieved all of the results and you can now close the query::

  $ curl -v -X DELETE http://localhost:10000/v3/data/explore/queries/07fd9b6a-95b3-4831-992c-7164f11c3754


.. Stopping and Removing the Application
.. =====================================
.. include:: _includes/_stopping-removing-application-title.txt

.. include:: _includes/_stopping-flow.txt

.. include:: _includes/_stopping-services.txt

.. include:: _includes/_removing-application.txt

