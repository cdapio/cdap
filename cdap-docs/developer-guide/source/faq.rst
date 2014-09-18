.. :author: Cask Data, Inc.
   :description: Frequently Asked Questions about the Cask Data Application Platform
   :copyright: Copyright © 2014 Cask Data, Inc.

==================================
Cask Data Application Platform FAQ
==================================

.. contents::
   :local:
   :class: faq
   :backlinks: none

.. _support: https://cask.uservoice.com/clients/widgets/classic_widget?mode=support&link_color=162e52&primary_color=42afcf&embed_type=lightbox&trigger_method=custom_trigger&contact_enabled=true&feedback_enabled=false&smartvote=true&referrer=http%3A%2F%2Fwww.cask.co%2Fdevelopers#faq


Cask Data Application Platform
==============================

What is the Cask Data Application Platform?
-------------------------------------------
Cask Data Application Platform (CDAP) is the industry’s first Big Data Application Server for Hadoop. It
abstracts all the complexities and integrates the components of the Hadoop ecosystem (YARN, MapReduce, 
Zookeeper, HBase, etc.) enabling developers to build, test, deploy, and manage Big Data applications
without having to worry about infrastructure, interoperability, or the complexities of distributed
systems.

What is available in the CDAP SDK?
-----------------------------------------
The CDAP SDK comes with:

- Java and RESTful APIs to build CDAP applications;
- Standalone CDAP to run the entire CDAP stack in a single Java virtual machine; and
- Example CDAP applications.

Why should I use Cask Data Application Platform for developing Big Data Applications?
-------------------------------------------------------------------------------------
CDAP helps developers to quickly develop, test, debug and deploy Big Data applications. Developers can
build and test Big Data applications on their laptop without need for any distributed environment to
develop and test Big Data applications. Deploy it on the distributed cluster with a push of a button. The
advantages of using CDAP include:

1. **Integrated Framework:**
   CDAP provides an integrated platform that makes it easy to create all the elements of Big Data
   applications: collecting, processing, storing, and querying data. Data can be collected and stored in
   both structured and unstructured forms, processed in real-time and in batch, and results can be made
   available for retrieval, visualization, and further analysis.

#. **Simple APIs:**
   CDAP aims to reduce the time it takes to create and implement applications by hiding the
   complexity of these distributed technologies with a set of powerful yet simple APIs. You don’t need to
   be an expert on scalable, highly-available system architectures, nor do you need to worry about the low
   level Hadoop and HBase APIs.

#. **Full Developement Lifecycle Support:**
   CDAP supports developers through the entire application development lifecycle: development, debugging,
   testing, continuous integration and production. Using familiar development tools like Eclipse and
   IntelliJ, you can build, test and debug your application right on your laptop with a Standalone CDAP. Utilize
   the application unit test framework for continuous integration.

#. **Easy Application Operations:**
   Once your Big Data application is in production, CDAP is designed specifically to monitor your
   applications and scale with your data processing needs: increase capacity with a click of a button
   without taking your application offline. Use the CDAP Console or RESTful APIs to monitor and manage the
   lifecycle and scale of your application.


Platforms and Language
======================

What Platforms are Supported by the Cask Data Application Platform SDK?
-----------------------------------------------------------------------
The CDAP SDK can be run on Mac OS X, Linux or Windows platforms.

What programming languages are supported by CDAP?
--------------------------------------------------------
CDAP currently supports Java. 

What Version of Java SDK is Required by CDAP?
------------------------------------------------------------
The latest version of the JDK or JRE version 6 or 7 must be installed in your environment.

What Version of Node.JS is Required by CDAP?
------------------------------------------------------------
The version of Node.js must be v0.8.16 or greater.


Hadoop
======

I have a Hadoop cluster in my datacenter, can I run CDAP that uses my Hadoop cluster?
---------------------------------------------------------------------------------------- 
Yes. You can install CDAP on your Hadoop cluster. Contact `Cask support`__ on how this can be done.

__ support_

What Hadoop distributions can CDAP run on? 
-------------------------------------------------
CDAP has been tested on and supports CDH 4.2.x or later, HDP 2.0 or later, and Apache Hadoop/HBase 2.0.2-0.4 and 2.1.0. 


Feedback and Support
====================

How can I Provide Feedback?
---------------------------
Feel free to provide feedback by contacting `Cask support`__.

__ support_

Any Unanswered Questions?
-------------------------
Please contact `Cask support`__ if you have any unanswered questions.

__ support_

