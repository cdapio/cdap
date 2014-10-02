..
   Copyright © 2012-2014 Cask Data, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

.. _gettin_started_toplevel:

.. index::
   single: CDAP Getting Started

===============
Getting Started
===============

Whatever you'll be building with CDAP in future, the getting started is designed to get you setup and be productive as quickly as possible using the latest CDAP standalone release recommended by Cask team.

What is required ?
==================
 * 10 minutes of your precious time
 * JDK 1.6 or later
 * Maven 3.0+
 * Node.js 0.8.16+ (required for only standalone ZIP installation.)

CDAP SDK Installation
=====================

The CDAP Software Development Kit (SDK) is all that is needed to develop CDAP applications in your development environment, either your laptop or a work station. It includes:

#. A Standalone CDAP that can run on a single machine in a single JVM. It provides all of the CDAP APIs without requiring a Hadoop cluster, using alternative, fully functional implementations of CDAP features. For example, application containers are implemented as Java threads instead of YARN containers.
#. The CDAP Console, a web-based graphical user interface to interact with the Standalone CDAP and the applications it runs.
#. The complete CDAP documentation, including this document and the Javadocs for the CDAP APIs.
#. A set of tools, datasets and example applications that help you get familiar with CDAP, and can also serve as templates for developing your own applications.

There are three different ways you can get CDAP SDK setup on machine. Depending on what you are comfortable with, please choose one of the following options 
to setup CDAP SDK.

.. toctree::
   :maxdepth: 1

   ../installation/standalone/zip 
   ../installation/standalone/vm 
   ../installation/standalone/docker 

Your first big data CDAP application
====================================

In order for you to get started with CDAP, we have a collection of pre-built applications that you can play with to experience the power of CDAP. For this demonstration
we will be using the most common big data application use-case of log analytics. Application aggregates logs, implements realtime and batch analytics of the logs ingested and surfaces the processed data using multiple interfaces. Application extracts processes web server access logs, counts visits by different IPs seen in the logs in real-time, and computes the bounce ratio of each web page encountered using batch processing. 

Download Application
--------------------

You can either choose to download the archived application source we have built for you or you can choose to pull it from github.
In both cases, the instruction for how to build is the same.::

  $ wget http://repository.cask.co/downloads/co/cask/cdap/apps/0.1.0/cdap-wise-0.1.0.zip
  $ unzip cdap-wise-0.1.0.zip

and if you were to clone from github, following are the commands::

  $ git clone https://github.com/caskdata/cdap-apps/tree/develop/Wise cdap-wise-0.1.0

Build Application
-----------------

Build CDAP application::

  $ cd cdap-wise-0.1.0
  $ mvn package

Deploy Application
------------------
Upon successful build, we will deploy the application into already configured CDAP instance.::

  $ bin/app-manager.sh --action deploy
  $ echo $?

Start Application
-----------------
Once, the application is deployed into CDAP, you are now ready to start the application.::

  $ bin/app-manager.sh --action start

For Windows environment you can use BAT equivalent of the script app-manager present in the BIN directory.

Interact with application
=============================

Like any other big data application, first thing you need to do is feed application with data. In this case, we are 
looking to inject Apache Access Log data. We have already prepared the data for you to try. So, please use the step 
below to inject data into CDAP::

  $ bin/inject-data.sh

The above command will take events from the prepackaged Apache Access Log that comes with the application and injects
it into CDAP. While the data is being injected, you can visit the application console to see metrics associated with 
the application ingestion and processing. Visit http://localhost:9999/#/apps/Wise

The application deployed will do it's magic and process the injected data both in Real-time and Batch and surfaces up 
through SQL like interface and also as REST APIs. All of the services are part of the application deployed. 

Like any application, it should have simpler ways for retrieving the processed data. CDAP provides multiple ways 
for retrieving the data - All of the methods are exposed through REST APIs.

So, now let's try to retrieve some of the interesting statisitics from the processed data.

You will be able to retrieve counts of number of times a given ip is observed in access log::

   $ curl http://localhost:10000/v2/apps/Wise/services/WiseService/methods/ip/255.255.255.170/count; echo

Another interesting query to retrieve the web pages from where IP addresses have bounced more than 50% of the time::

   $ curl -d '{ "query" : "SELECT uri FROM cdap_user_bouncecountstore WHERE bounces > 0.1 * totalvisits" }' http://localhost:10000/v2/data/explore/queries
     { '<id>' }
   $ curl -X POST http://localhost:10000/v2/data/explore/queries/<id>/download
     
Another query is to retrieve all the IP addresses which visited the page ‘/contact.html’::

   $ curl -d '{ SELECT key FROM cdap_user_pageviewstore WHERE array_contains(map_keys(value), '/contact.html')=TRUE }' http://localhost:10000/v2/data/explore/queries
     { '<id>' }
   $ curl -X POST http://localhost:10000/v2/data/explore/queries/<id>/download

Summary
=======
Congratulations! You've just successfully run a big data log analytics application on CDAP. You can deploy the same application 
on a real cluster and experience the same power of CDAP. If you are interested in going deeper into how the application is built 
using CDAP APIs - please check application tutorial here. We also have other applications that will help you familize more with 
CDAP here.

