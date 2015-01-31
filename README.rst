=====================================
Cask Data Application Platform - CDAP
=====================================

.. image:: cdap-docs/developers-manual/source/_images/CDAP.png
   :align: left
   :alt: CDAP Logo

Introduction
============

The Cask Data Application Platform (CDAP) is an application server that provides a
comprehensive platform for the development, deployment and management of data applications 
and the management of data.

Out-of-the-box, its features include transaction management, dataset abstractions, QoS
(quality-of-service), performance, scalability, security, metrics and log collection, and
a web-based management console.

CDAP provides data virtualization and application containerization for your data and
application needs. With CDAP, you do not need to understand the implementation or the
complexity of Apache Hadoop&trade;, HBase or Zookeeper. CDAP provides independence from
Hadoop versions and runs on any distribution of Hadoop. Its container model allows for the
integration of different processing paradigms with CDAP's features. It provides a common
environment, the abstraction of a unified API, lifecycle management and a programming
model for both data applications and their data. You can package, deploy and manage
applications as a single unit.

You can run applications ranging from simple MapReduce Jobs through complete ETL (extract,
transform, and load) pipelines all the way up to complex, enterprise-scale data-intensive
applications. Developers can build and test their applications end-to-end in a full-stack,
single-node installation. CDAP can be run either standalone, deployed within the
Enterprise or hosted in the Cloud.

For more information, see our collection of 
`Developers' Manual and other documentation 
<http://docs.cask.co/cdap/current/en/developers-manual/index.html`__.


Getting Started
===============

Prerequisites
-------------
To install and use CDAP and its included examples, there are a few simple prerequisites:

  1. JDK 6 or JDK 7 (required to run CDAP; note that $JAVA_HOME should be set)
  2. Node.js 0.8.16+ (required to run the CDAP Console)
  3. Apache Maven 3.0+ (required to build the example applications)
  
Build
-----
You can get started with CDAP by building directly from the latest source code::

  git clone https://github.com/caskdata/cdap.git
  cd cdap
  mvn clean package

After the build completes, you will have a distribution of the CDAP standalone under the
``cdap-distribution/target/`` directory.  

Take the ``cdap-<version>.tar.gz`` file and unzip it into a suitable location.

For more build options, please refer to the `build instructions <BUILD.md`__.

Getting Started Guide
---------------------
Visit our web site for a 
`Getting Started <http://docs.cask.co/cdap/current/en/developers-manual/getting-started/index.html>`__
that will guide you through installing CDAP and running an example application.  


Where to Go Next
================
Now that you've had a look at the CDAP SDK, take a look at:

- Examples, located in the ``cdap-examples`` directory of the CDAP SDK;
- `Selected Examples <http://docs.cask.co/cdap/current/en/examples-manual/examples/index.html>`__ 
  (demonstrating basic features of the CDAP) are located on-line; and
- Developers' Manual, located in the source distribution in ``cdap-docs/developers-manual/source``
  or `online <http://docs.cask.co/cdap/current/en/developers-manual/index.html`__.


How to Contribute
=================

Interested in helping to improve CDAP? We welcome all contributions, whether in filing detailed
bug reports, submitting pull requests for code changes and improvements, or by asking questions and
assisting others on the mailing list.

For quick guide to getting your system setup to contribute to CDAP, take a look at our 
`Contributor Quickstart Guide <DEVELOPERS.rst>`__.

Bug Reports & Feature Requests
------------------------------
Bugs and tasks are tracked in a public JIRA issue tracker. Details on access will be forthcoming.

Pull Requests
-------------
We have a simple pull-based development model with a consensus-building phase, similar to Apache's
voting process. If you’d like to help make CDAP better by adding new features, enhancing existing
features, or fixing bugs, here's how to do it:

1. If you are planning a large change or contribution, discuss your plans on the `cask-cdap-dev`
   mailing list first.  This will help us understand your needs and best guide your solution in a
   way that fits the project.
2. Fork CDAP into your own GitHub repository.
3. Create a topic branch with an appropriate name.
4. Work on the code to your heart's content.
5. Once you’re satisfied, create a pull request from your GitHub repo (it’s helpful if you fill in
   all of the description fields).
6. After we review and accept your request, we’ll commit your code to the cask/cdap
   repository.

Thanks for helping to improve CDAP!

Filing Issues
-------------
JIRA for filing [Issues](http://issues.cask.co)

Mailing Lists
-------------
CDAP User Group and Development Discussions:

- `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`__

  The *cdap-user* mailing list is primarily for users using the product to develop
  applications. You can expect questions from users, release announcements, and any other
  discussions that we think will be helpful to the users.

- `cdap-dev@googlegroups.com <https://groups.google.com/d/forum/cdap-dev>`__

  The *cdap-dev* mailing list is essentially for developers actively working
  on the product, and should be used for all our design, architecture and technical
  discussions moving forward. This mailing list will also receive all JIRA and GitHub
  notifications.

IRC Channel
-----------
CDAP IRC Channel: #cdap on irc.freenode.net


License and Trademarks
======================

Copyright © 2014 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the 
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
either express or implied. See the License for the specific language governing permissions 
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.
