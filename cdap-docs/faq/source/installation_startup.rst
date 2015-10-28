.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

:titles_only-toc: true

.. _faq-installation-startup:

==================================
CDAP FAQ: Installation and Startup
==================================

.. contents::
   :depth: 2
   :local:
   :backlinks: entry
   :class: faq

Installation
============

Building CDAP from Source
-------------------------
**Question:** We are trying to build CDAP from the source code, following the instructions given in
BUILD.rst, but are unable find the results. Our build commands are::

  mvn package -e -X -DskipTests -Dcheckstyle.skip
  mvn package -e -X -DskipTests -Dcheckstyle.skip -P examples,templates,dist,release,tgz,unit-tests

With these commands we are able to build (the maven build ends with SUCCESS status) but there is
no ``cdap-<version>.tar.gz`` file found at ``cdap-distribution/target/`` as mentioned in the README.rst.

-----

**Answer:** The first of these commands builds all modules, skipping tests and checkstyle, but does not build a distribution.
The second command is the correct command, and it creates multiple output files (``tar.gz``\ ), located in
the ``target`` directory inside each of ``cdap-master``, ``cdap-kafka``, ``cdap-gateway``, and ``cdap-ui``.


Memory and CPU Requirements
---------------------------
**Question:** How much memory and how many CPU cores are required for CDAP services running on HDP
(Horton Data Platform)? We're seeing eight containers being used for the CDAP master.service.

-----

**Answer:** The settings are governed by two sources: CDAP and YARN.

The default setting for CDAP are found in the ``cdap-defaults.xml``, and are over-ridden in
particular instances by the ``cdap-site.xml`` file. These vary with each service and range
from 512 to 1024 MB and from one to two cores.

The YARN settings will over-ride these; for instance, the minimum YARN container size is
determined by ``yarn.scheduler.minimum-allocation-mb``. The YARN default in ``HDP/Hadoop`` is 1024
MB, so containers will be allocated with 1024 MB, even if the CDAP settings are for 512
MB.


Installing CDAP on Cloudera
---------------------------
**Question:** I am experiencing problems installing CDAP on Cloudera Live on AWS. Following 
:ref:`the tutorial <step-by-step-cloudera-add-service>`,
when trying to start services, I received the following error in stderr::

  Error found before invoking supervisord: No parcel provided required tags: set([u'cdap'])

-----

**Answer:** Start by clicking on the parcel icon (near the top-left corner of Cloudera Manager; looks
like a gift-wrapped box), and ensuring that the CDAP parcel is listed as *Active*.

There are 4 steps to installing a parcel:
* Adding the repository to the list of repositories searched by Cloudera Manager
* "Downloading" the parcel to the Cloudera Manager server
* "Distributing" the parcel to all the servers in the cluster
* "Activating" the parcel

The error message suggests that you have not completed the last step, *Activation*.


Upgrading CDAP
--------------

**Question:** Can a current CDAP installation be upgraded more than one version?

-----

**Answer:** This table lists the upgrade paths available for different CDAP versions:

+---------+---------------------+
| Version | Upgrade Directly To |
+=========+=====================+
| 2.6.3   | **2.8.1**           |
+---------+---------------------+

If you are doing a new installation, we recommend using the current version of CDAP.


Where is the CDAP CLI (Command Line Interface)?
-----------------------------------------------
**Question:** We've installed CDAP on a cluster using RPM, and wanted to use the CDAP CLI, but couldn't find it.

-----

**Answer:** If you've installed the ``cdap-cli`` RPM, it's located under ``/opt/cdap/cli/bin``.

You can add this location to your PATH to prevent the need for specifying the entire script every time.

**Note:** This command will list the contents of the package ``cdap-cli``, once it has
been installed::

  rpm -ql cdap-cli


Distributed Mode: Are Two Machines Really Required?
---------------------------------------------------
**Question:** I've installed CDAP and following the installation instructions, each
component is installed onto two machines.  I'm not using the CDAP Authentication Server at
this point to minimize the moving parts.  Is it really necessary to install all components
on both machines?  Could I instead install just the web app on a third Node and the other
components on the second Node?  Could I install each component on a separate machine if I
chose to? The :ref:`HA [High Availability] Environment diagram
<deployment-architectures-ha>` seems to indicate this.

-----

**Answer:** The CDAP components are independently scalable, so you can install from 1 to N
of each component on any combination of nodes.  The primary reasons to do so are for HA,
and for cdap-router's data ingest capacity.


Distributed Mode: Port 10000 Already In Use
-------------------------------------------
**Question:** Port 10000 was being used by another service so I changed ``router.server.port``
to 10023.

-----

**Answer:** In the Hadoop ecosystem, Hive Server2 defaults to 10000. As a consequence, we
are considering changing the router default port. However, you can set it to whatever you
need and specify it in the cdap-site.xml.

Distributed Mode: Multiple Properties Specifying an IP Address
--------------------------------------------------------------
**Question:** Several properties specify an IP where a service is running, such as:
``router.server.address``, ``metrics.query.bind.address``, ``data.tx.bind.address``, ``app.bind.address``,
``router.bind.address``. What do I set these to if the components are running on multiple
machines?

-----

**Answer:** Our convention is that '*.bind.*' properties are what services use during startup to
listen on a particular interface/port.  '*.server.*' properties are used by clients to
connect to another (potentially remote) service.  

For '\*.bind.address' properties, it is often easiest just to set these to '0.0.0.0' to
listen on all interfaces.   

The '\*.server.\*' properties are used by clients to connect to another remote service. The
only one you should need to configure initially is router.server.address, which is used by
the UI to connect to the router.  As an example, ideally routers running in production
would have a load balancer in front, which is what you would set router.server.address to.
Alternatively, you could configure each UI instance to point to a particular router, and
if you have both UI and router running on each node, you could use '127.0.0.1'.


Startup
=======

Error with CDAP SDK on Start-up
-------------------------------
**Question:** I've downloaded an SDK package (cdap-sdk-3.1.0.zip) from the cask.co website, and have installed it
on a CDH 5 data node with CentOS 6.5, JDK 1.7, node.js and maven 3.3.3. I'm seeing this error on startup::

  ERROR [main:c.c.c.StandaloneMain@268] - Failed to start Standalone CDAP
  java.lang.NoSuchMethodError: co.cask.cdap.UserInterfaceService.getServiceName()Ljava/lang/String;
    at co.cask.cdap.UserInterfaceService.access$000(UserInterfaceService.java:44) ~[co.cask.cdap.cdap-standalone-3.1.0.jar:na]
    ...
  	at co.cask.cdap.StandaloneMain.main(StandaloneMain.java:265) ~[co.cask.cdap.cdap-standalone-3.1.0.jar:na]  

-----

**Answer:** You've downloaded the standalone version of CDAP. **It's not intended to be run on Hadoop clusters.**

Instead, you might want to download the CDAP CSD for Cloudera Manager, either from 
http://cloudera.com/downloads or http://cask.co/downloads. Using the CSD, you will be able to install CDAP on CDH.

In addition, the stack trace suggests that the JAVA_HOME is pointing to 1.6, rather than
1.7. The minimum version of Java supported by CDAP is 1.7. Echo ``$JAVA_HOME`` and adjust
it as required.


Starting Standalone CDAP: It Failed to Start
--------------------------------------------
**Question:** When I start the CDAP Standalone, it fails to start. In the CDAP log, I'm seeing this error message::

  2015-05-15 12:15:53,028 - ERROR [heartbeats-scheduler:c.c.c.d.s.s.MDSStreamMetaStore$1@71] - Failed to access app.meta table
  co.cask.cdap.data2.dataset2.DatasetManagementException: Cannot retrieve dataset instance app.meta info,
  details: Response code: 407, message:'Proxy Authentication Required', body: '<HTML><HEAD>
  <TITLE>Access Denied</TITLE>
  </HEAD>

  Your credentials could not be authenticated: "Credentials are missing.". 
  You will not be permitted access until your credentials can be verified.

  This is typically caused by an incorrect username and/or password, 
  but could also be caused by network problems.
  
  For assistance, contact your network support team.
  
  at co.cask.cdap.data2.datafabric.dataset.DatasetServiceClient.getInstance(DatasetServiceClient.java:104)
  ...
  
I am running from behind a corporate poxy host, in case that's an issue.

-----

**Answer:** According to that log, this is indeed caused by the proxy setting. 

CDAP services internally makes HTTP requests to each other; one example is the dataset
service. Depending on your proxy and its settings, these requests can end up being sent to
the proxy instead.

One item to check is that your system's network setting is configured to exclude both
localhost and 127.0.0.1 from the proxy routing. If they aren't, the services will not be
able to communicate with each other, and you'll see error messages such as these.


