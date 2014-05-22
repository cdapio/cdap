.. .. _faq_toplevel:

.. toctree::
   :hidden:
   :maxdepth: 0
   
==========================
FAQ
==========================

.. contents::
   :local:
   :class: faq
   :backlinks: none

.. _support: https://continuuity.uservoice.com/clients/widgets/classic_widget?mode=support&link_color=162e52&primary_color=42afcf&embed_type=lightbox&trigger_method=custom_trigger&contact_enabled=true&feedback_enabled=false&smartvote=true&referrer=http%3A%2F%2Fwww.continuuity.com%2Fdevelopers#faq


Continuuity Reactor
===================

What is a Continuuity Reactor?
------------------------------
Continuuity Reactor is the industry’s first Big Data Application Server for Hadoop. It abstracts all the complexities and integrates the components of the Hadoop ecosystem (YARN, MapReduce, Zookeeper, HBase, etc.) enabling developers to build, test, deploy, and manage Big Data applications without having to worry about infrastructure, interoperability, or the complexities of distributed systems.

What is available in the Continuuity SDK?
-----------------------------------------
Continuuity SDK comes with:

- Java and REST APIs to build Continuuity Reactor applications;
- Local Reactor to run the entire Reactor stack in a single Java virtual machine; and
- Example Reactor applications.

Why should I use Continuuity Reactor for developing Big Data Applications?
--------------------------------------------------------------------------
Reactor helps developers to quickly develop, test, debug and deploy Big Data applications. Developers can build and test Big Data applications on their laptop without need for any distributed environment to develop and test Big Data applications. Deploy it on the distributed cluster with a push of a button. The advantages of using Reactor includes:

1. Integrated Framework 
Reactor provides an integrated platform that makes it easy to create all the elements of Big Data applications: collecting, processing, storing, and querying data. Data can be collected and stored in both structured and unstructured forms, processed in real-time and in batch, and results can be made available for retrieval, visualization, and further analysis.

2. Simple APIs
Continuuity Reactor aims to reduce the time it takes to create and implement applications by hiding the complexity of these distributed technologies with a set of powerful yet simple APIs. You don’t need to be an expert on scalable, highly-available system architectures, nor do you need to worry about the low-level Hadoop and HBase APIs.

3. Full Developement Lifecycle Support
Reactor supports developers through the entire application development lifecycle: development, debugging, testing, continuous integration and production. Using familiar development tools like Eclipse and IntelliJ, you can build, test and debug your application right on your laptop with a Local Reactor. Utilize the application unit test framework for continuous integration.

4. Easy Application Operations
Once your Big Data application is in production, Continuuity Reactor is designed specifically to monitor your applications and scale with your data processing needs: increase capacity with a click of a button without taking your application offline. Use the Reactor dashboard or REST APIs to monitor and manage the lifecycle and scale of your application.


Platforms and Language
======================

What Platforms are Supported by the Continuuity SDK?
----------------------------------------------------
The Development Kit can be run on Mac OS X, Linux or Windows platforms.

What programming languages are supported by the Reactor?
--------------------------------------------------------
Reactor currently supports Java. 

What Version of Java SDK is Required by the Continuuity SDK?
------------------------------------------------------------
The latest version of the JDK or JRE version 6 or 7 must be installed in your environment.

What Version of Node.JS is Required by the Continuuity SDK?
------------------------------------------------------------
The version of Node.js must be v0.8.16 or greater.


Sandbox Reactor
===============

What is a Sandbox Reactor?
--------------------------
A Sandbox Reactor is a self-service Continuuity Reactor to deploy, manage and monitor Reactor applications in the cloud. A Sandbox Reactor is deployed on a single instance, hosted by Continuuity.com and available for 30 days for free. 

How do I Get a Sandbox Reactor?
-------------------------------
Anyone who wishes to get a Sandbox Reactor can signup with `Continuuity <https://accounts.continuuity.com/signup>`_ and provision a Sandbox Reactor for 30 days.

How Long Does It Take to Provision a Sandbox Reactor?
-----------------------------------------------------
Sandbox creation can take several minutes. The normal time is between ten to twenty minutes but time varies based on how many users are provisioning at a given time. We've recently observed longer delays because of high demand during peak hours; provisioning taking up to 30 minutes has been observed. If no confirmation is received within an hour, please contact `Continuuity support`__.

__ support_

Is My Sandbox Reactor Accessible or Visible to Other Users?
-----------------------------------------------------------
Sandboxes are isolated from other accounts and are only visible to the account owner.

What Happens to My Sandbox Reactor After the 30-day Trial?
----------------------------------------------------------
The Sandbox Reactor will be deleted along with any application and data that is stored during the 30-day trial. Continuuity does not backup the applications that are deployed or the data that is stored.

After the 30-day Trial, can I Convert my Sandbox Reactor into a Production Environment?
---------------------------------------------------------------------------------------
Yes. To do so either contact `Continuuity support`__,
or email `sales@continuuity.com <mailto:sales@continuuity.com>`__.

__ support_


Hadoop
======

I have a Hadoop cluster in my datacenter, can I run Reactor that uses my Hadoop cluster?
---------------------------------------------------------------------------------------- 
Yes. You can install Reactor on your Hadoop cluster. Contact `Continuuity support`__ on how this can be done.

__ support_

What Hadoop distributions can the Reactor run on? 
-------------------------------------------------
Continuuity Reactor has been tested on and supports CDH 4, HDP 2.1, and Apache Hadoop/HBase 2.0.2-0.4 and 2.1.0. 


Feedback and Support
====================

How can I Provide Feedback?
---------------------------
Feel free to provide feedback by contacting `Continuuity support`__.

__ support_

Any Unanswered Questions?
-------------------------
Please contact `Continuuity support`__ if you have any unanswered questions.

__ support_

