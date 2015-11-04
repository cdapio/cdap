.. meta::
    :author: Cask Data, Inc.
    :description: Frequently Asked Questions about the Cask Data Application Platform
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

:hide-nav: true
:orphan:

========
CDAP FAQ
========

.. contents::
   :depth: 2
   :local:
   :backlinks: entry
   :class: faq

Cask Data Application Platform
==============================

What is the Cask Data Application Platform?
-------------------------------------------
Cask Data Application Platform (CDAP) is the industry’s first Big Data Application Server for Hadoop. It
abstracts all the complexities and integrates the components of the Hadoop ecosystem (YARN, MapReduce,
ZooKeeper, HBase, etc.) enabling developers to build, test, deploy, and manage Big Data applications
without having to worry about infrastructure, interoperability, or the complexities of distributed
systems.

What is available in the CDAP SDK?
----------------------------------
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
   CDAP provides an integrated platform that makes it easy to create all the functions of Big Data
   applications: collecting, processing, storing, and querying data. Data can be collected and stored in
   both structured and unstructured forms, processed in real time and in batch, and results can be made
   available for retrieval, visualization, and further analysis.

#. **Simple APIs:**
   CDAP aims to reduce the time it takes to create and implement applications by hiding the
   complexity of these distributed technologies with a set of powerful yet simple APIs. You don’t need to
   be an expert on scalable, highly-available system architectures, nor do you need to worry about the low
   level Hadoop and HBase APIs.

#. **Full Development Lifecycle Support:**
   CDAP supports developers through the entire application development lifecycle: development, debugging,
   testing, continuous integration and production. Using familiar development tools like Eclipse and
   IntelliJ, you can build, test and debug your application right on your laptop with a Standalone CDAP. Utilize
   the application unit test framework for continuous integration.

#. **Easy Application Operations:**
   Once your Big Data application is in production, CDAP is designed specifically to monitor your
   applications and scale with your data processing needs: increase capacity with a click of a button
   without taking your application offline. Use the CDAP UI or RESTful APIs to monitor and manage the
   lifecycle and scale of your application.


Platforms and Language
======================

What platforms are supported by the Cask Data Application Platform SDK?
-----------------------------------------------------------------------
The CDAP SDK can be run on Mac OS X, Linux, or Windows platforms.

What programming languages are supported by CDAP?
-------------------------------------------------
CDAP currently supports Java for developing applications.

What version of Java SDK is required by CDAP?
---------------------------------------------
The latest version of the JDK or JRE version 7 or version 8 must be installed
in your environment; we recommend the Oracle JDK.

What version of Node.js is required by CDAP?
--------------------------------------------
The version of `Node.js <https://nodejs.org/>`__ must be from |node-js-version|; we recommend |recommended-node-js-version|.


Hadoop
======

I have a Hadoop cluster in my data center, can I run CDAP that uses my Hadoop cluster?
--------------------------------------------------------------------------------------
Yes. You can install CDAP on your Hadoop cluster. See :ref:`install`.

What Hadoop distributions can CDAP run on?
------------------------------------------
CDAP |version| has been tested on and supports CDH 5.0.0 through 5.4.x; HDP 2.0 through 2.3;
MapR 4.1 and 5.0, and Apache Bigtop 0.8.0.


.. _faq-cdap-user-groups:

Issues, User Groups, Mailing Lists, IRC Channel
===============================================

I've found a bug in CDAP. How do I file an issue?
-------------------------------------------------
We have a `JIRA for filing issues <https://issues.cask.co/browse/CDAP>`__.

What User Groups and Mailing Lists are available about CDAP?
------------------------------------------------------------
- `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`__

The *cdap-user* mailing list is primarily for users using the product to develop
applications. You can expect questions from users, release announcements, and any other
discussions that we think will be helpful to the users.

- `cdap-dev@googlegroups.com <https://groups.google.com/d/forum/cdap-dev>`__

The *cdap-dev* mailing list is essentially for developers actively working
on the product, and should be used for all our design, architecture and technical
discussions moving forward. This mailing list will also receive all JIRA and GitHub
notifications.

Is CDAP on IRC?
---------------
**CDAP IRC Channel:** #cdap on `chat.freenode.net <irc://chat.freenode.net:6667/cdap>`__.


Startup
=======

CDAP installed on CDH using Cloudera Manager doesn't startup |---| what do I do?
--------------------------------------------------------------------------------
A :ref:`tutorial <step-by-step-cloudera-add-service>` is available with instructions on how to install CDAP on CDH
(`Cloudera Data Hub <http://www.cloudera.com/content/www/en-us/resources/datasheet/cdh-datasheet.html>`__)
using `Cloudera Manager <http://www.cloudera.com/content/www/en-us/products/cloudera-manager.html>`__.

If, when you try to start services, you receive an error in ``stderr`` such as::

  Error found before invoking supervisord: No parcel provided required tags: set([u'cdap'])

The error message shows that that a required parcel isn't available, suggesting that you
have not completed the last step of installing a parcel, *Activation*. There are 4 steps
to installing a parcel:

- **Adding the repository** to the list of repositories searched by Cloudera Manager
- **Downloading** the parcel to the Cloudera Manager server
- **Distributing** the parcel to all the servers in the cluster
- **Activating** the parcel

Start by clicking on the parcel icon (near the top-left corner of Cloudera Manager and looks
like a gift-wrapped box) and ensure that the CDAP parcel is listed as *Active*.


I've followed the install instructions, yet CDAP does not start and fails verification. What next?
--------------------------------------------------------------------------------------------------
If you have followed :ref:`the installation instructions <install>`, and CDAP either did not pass the
:ref:`verification step <configuration-verification>` or did not startup, check:

- Look in the CDAP logs for error messages (located either in ``/var/log/cdap`` for CDAP Distributed or
  ``$CDAP_HOME/logs`` for CDAP SDK Standalone)
- If you see an error such as::

    ERROR [main:c.c.c.StandaloneMain@268] - Failed to start Standalone CDAP
    java.lang.NoSuchMethodError:
    co.cask.cdap.UserInterfaceService.getServiceName()Ljava/lang/String

  then you have downloaded the standalone version of CDAP, which is not intended
  to be run on Hadoop clusters. Download the appropriate distributed packages (RPM or
  Debian version) from http://cask.co/downloads.

- Check permissions of directories:

  - The :ref:`CDAP HDFS User <configuration-options>` (by default, ``yarn``) owns the HDFS directory (by default,  ``/cdap``).
  - The :ref:`Kafka Log directory <configuration-options>` (by default, ``/data/cdap/kafka-logs``), must be writable by the default CDAP user.
  - The :ref:`temp directories <configuration-tmp-files>` utilized by CDAP must be writable by the default CDAP user.

.. - Check :ref:`configuration troubleshooting <configuration-troubleshooting>` suggestions


The CDAP UI is showing a message "namespace cannot be found".
-------------------------------------------------------------
This is indicative that the UI cannot connect to the CDAP system service containers running in YARN.

- First, check if the CDAP Master service container shows as RUNNING in the YARN ResourceManager UI.
  The CDAP Master, once it starts, starts the other CDAP system service containers, so if it isn't running,
  the others won't be able to start or work correctly. It can take several minutes for everything to start up.

- If this doesn't resolve the issue, then it means the CDAP system services were unable to launch.
  Ensure :ref:`YARN has enough spare memory and vcore capacity <faq-installation-startup-memory-core-requirements>`. 
  CDAP attempts to launch between 8 and 11 containers, depending on the configuration. Check
  the master container (Application Master) logs to see if it was able to launch all containers.

- If it was able to launch all containers, then you may need to check the launched container logs for any errors.
  The ``yarn-site.xml`` configuration file determines the container log directory.


I don't see the CDAP Master service on YARN.
--------------------------------------------
- Ensure that the node where CDAP is running has a properly configured YARN client.
- Ensure :ref:`YARN has enough memory and vcore capacity <faq-installation-startup-memory-core-requirements>`.
- Is the router address properly configured in the :ref:`cdap-site.xml file <configuration-options>` and the boxes using it?
- Check that the classpath used includes the YARN configuration in it.


The CDAP Master log shows permissions issues.
---------------------------------------------
Ensure that ``hdfs:///${hdfs.namespace}`` and ``hdfs:///user/${hdfs.user}`` exist and are owned by ``${hdfs.user}``.
(``hdfs.namespace`` and ``hdfs.user`` are defined in your installation's :ref:`cdap-site.xml file <configuration-options>`.)

In rare cases, ensure ``hdfs:///${hdfs.namespace}/tx.snapshot`` exists and is owned by
``${hdfs.user}``, until `CDAP-3817 <https://issues.cask.co/browse/CDAP-3817>`__ is
resolved.

In any other case, the error should show which directory it is attempting to access, such as::

  2015-10-30 22:14:27,528 - ERROR [ STARTING:...MasterServiceMain$2@452] - master.services failed with exception; restarting with back-off
  java.lang.RuntimeException: java.io.IOException: failed to copy bundle from file:/tmp/appMaster.37a86cfd....jar5052.tmp
  to hdfs://nameservice/cdap/twill/master.services/b4ce41a5e7e5.../appMaster.37a86cfd-1d88.jar
  at com.google.common.base.Throwables.propagate(Throwables.java:160) ~[com.google.guava.guava-13.0.1.jar:na]
  ...
  Caused by: org.apache.hadoop.ipc.RemoteException(org.apache.hadoop.security.AccessControlException):
  Permission denied: user=yarn, access=WRITE, inode="/":hdfs:supergroup:drwxr-xr-x
  at org.apache.hadoop.hdfs.server.namenode.FSPermissionChecker.checkFsPermission(FSPermissionChecker.java:271)
  at org.apache.hadoop.hdfs.server.namenode.FSPermissionChecker.check(FSPermissionChecker.java:257)

or::

  Deploy failed: Could not create temporary directory at: /var/tmp/cdap/data/namespaces/phoenix/tmp

Don't hesitate to ask for help at the `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`__.


The CDAP Master log shows an error about the dataset service not being found.
-----------------------------------------------------------------------------
If you see an error such as::

    2015-05-15 12:15:53,028 - ERROR [heartbeats-scheduler:c.c.c.d.s.s.MDSStreamMetaStore$1@71]
    - Failed to access app.meta table co.cask.cdap.data2.dataset2.DatasetManagementException:
    Cannot retrieve dataset instance app.meta info, details: Response code: 407,
    message:'Proxy Authentication Required',
    body: '<HTML><HEAD> <TITLE>Access Denied</TITLE> </HEAD>

According to that log, this error can be caused by a proxy setting. CDAP services
internally makes HTTP requests to each other; one example is the dataset service.
Depending on your proxy and its settings, these requests can end up being sent to the
proxy instead.

One item to check is that your system's network setting is configured to exclude both
``localhost`` and ``127.0.0.1`` from the proxy routing. If they aren't, the services will
not be able to communicate with each other, and you'll see error messages such as these.
You can set a system's network setting for a proxy by using::

  export no_proxy="localhost,127.0.0.1"


Where is the CDAP CLI (Command Line Interface)?
-----------------------------------------------
If you've installed the ``cdap-cli`` RPM or DEB, it's located under ``/opt/cdap/cli/bin``.
If you have installed CDAP manually (without using Cloudera Manager or Apache Ambari),
you can add this location to your PATH to prevent the need for specifying the entire script every time.

**Note:** These commands will list the contents of the package ``cdap-cli``, once it has
been installed::

  rpm -ql cdap-cli
  dpkg -L cdap-cli


.. _faq-installation-startup-memory-core-requirements:

What are the memory and core requirements for CDAP?
---------------------------------------------------
The settings are governed by two sources: CDAP and YARN, and the requirements are
:ref:`described here <install-hardware-memory-core-requirements>`.


Can a current CDAP installation be upgraded more than one version?
------------------------------------------------------------------
In general, no. (The exception is an upgrade from 2.8.x to 3.0.x.)
This table lists the upgrade paths available for different CDAP versions:

+---------+---------------------+
| Version | Upgrade Directly To |
+=========+=====================+
| 3.1.x   | 3.2.x               |
+---------+---------------------+
| 3.0.x   | 3.1.x               |
+---------+---------------------+
| 2.8.x   | 3.0.x               |
+---------+---------------------+
| 2.6.3   | 2.8.2               |
+---------+---------------------+

If you are doing a new installation, we recommend using the current version of CDAP.


Configuring Distributed Mode
============================

Are at least two machines really required for CDAP services?
------------------------------------------------------------
The CDAP components are independently scalable, so you can install from 1 to *N* of each
component on any combination of nodes.  The primary reasons for using at least two
machines are for HA (high availability) and for ``cdap-router``'s data ingest capacity.

It is not necessary to install all components on both machines; you could install just the
CDAP UI on a third machine with other components on the second node. You can install each
component on a separate machine (or more) if you choose. The :ref:`HA [High Availability]
Environment diagram <deployment-architectures-ha>` gives just one possible
configuration.


The HiveServer2 defaults to 10000; what should I do?
----------------------------------------------------
By default, CDAP uses port 10000. If port 10000 is being used by another service, simply
change the ``router.bind.port`` in the ``cdap-site.xml`` to another available port. Since
in the Hadoop ecosystem, HiveServer2 defaults to 10000, we are considering
`changing the router default port <https://issues.cask.co/browse/CDAP-1696>`__.

If you use Apache Ambari to install CDAP, it will detect this and run the CDAP Router on
port 11015. Another solution is to simply run the CDAP Router on a different host than
HiveServer2.


How do I set the CDAP properties for components running on multiple machines?
-----------------------------------------------------------------------------
In the configuration file ``cdap-site.xml``, there are numerous properties that specify an
IP address where a service is running, such as ``router.server.address``,
``metrics.query.bind.address``, ``data.tx.bind.address``, ``app.bind.address``,
``router.bind.address``.

Our convention is that:

- *\*.bind.\** properties are what services use during startup to listen on a particular interface/port. 
- *\*.server.\** properties are used by clients to connect to another (potentially remote) service.

For *\*.bind.address* properties, it is often easiest just to set these to ``'0.0.0.0'``
to listen on all interfaces.

The *\*.server.\** properties are used by clients to connect to another remote service.
The only one you should need to configure initially is ``router.server.address``, which is
used by the UI to connect to the router.  As an example, ideally routers running in
production would have a load balancer in front, which is what you would set
``router.server.address`` to. Alternatively, you could configure each UI instance to point
to a particular router, and if you have both UI and router running on each node, you could
use ``'127.0.0.1'``.


Additional Resources
====================

Check our issues database for known issues
------------------------------------------
When trying to solve an issue, one source of information is the CDAP Issues database.
The `unresolved issues can be browsed
<https://issues.cask.co/issues/?jql=project%3DCDAP%20AND%20resolution%3DUnresolved%20ORDER%20BY%20priority%20DESC>`__;
and using the search box in the upper-right, you can look for issues that contain a particular problem or keyword:

.. image:: _images/faq-quick-search.png
   :align: center

Ask the CDAP Community for assistance
-------------------------------------
You can post a question at the `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`__.

The *cdap-user* mailing list is primarily for users using the product to develop
applications. You can expect questions from users, release announcements, and any other
discussions that we think will be helpful to the users.
