======================================
Cask Data Application Platform Example
======================================

This ``example`` directory contains an example app for the Cask Data Application Platform
(CDAP). Detailed information about this example is available at the Cask documentation website:

  http://docs.cask.co/cdap/current/en/examples-manual/examples/index.html

A complete list of example applications is available in the parent directory.


Building
========

The example comes with a Maven pom.xml file. To build, install Maven, and from the
directory prompt, enter::

  mvn clean package


Summary of Application
======================

Purchase
--------
- An app that uses a scheduled Workflow to start a MapReduce that reads from one ObjectStore 
  Dataset and writes to another. The app also demonstrates using ad-hoc SQL queries, the
  RESTful interface of a Service, and custom Datasets.

  - Send sentences of the form "Tom bought 5 apples for $10" to the purchaseStream.
  - The PurchaseFlow reads the purchaseStream and converts every input String into a
    Purchase object and stores the object in the purchases Dataset.
  - When scheduled by the PurchaseHistoryWorkFlow, the PurchaseHistoryBuilder MapReduce
    program reads the purchases Dataset, creates a purchase history, and stores the purchase
    history in the history Dataset every morning at 4:00 A.M. You can manually (in the
    Process screen in the CDAP Console) or programmatically execute the 
    PurchaseHistoryBuilder MapReduce to store customers' purchase history in the
    history Dataset.
  - Request the ``PurchaseHistoryService`` retrieve from the *history* Dataset the purchase 
    history of a user.
  - You can use SQL to formulate ad-hoc queries over the history Dataset. This is done by
    a series of ``curl`` calls, as described in the RESTful API section of the Developers'
    Manual.

  - Note: Because by default the PurchaseHistoryWorkFlow process doesn't run until 4:00 A.M.,
    you'll have to wait until the next day (or manually or programmatically execute the
    PurcaseHistoryBuilder) after entering the first customers' purchases or the PurchaseQuery
    will return a "not found" error.



Cask is a trademark of Cask Data, Inc. All rights reserved.

Copyright © 2014-2015 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
except in compliance with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
either express or implied. See the License for the specific language governing permissions
and limitations under the License.
