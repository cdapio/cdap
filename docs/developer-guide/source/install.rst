.. :Author: John Jackson
   :Description: Installation guide for Continuuity Reactor on Linux systems

==============================================================
Continuuity Reactor 2.1.0 Installation and Configuration Guide
==============================================================

.. reST Editor: section-numbering::

.. reST Editor: contents::


Introduction
============

This guide is to help you install and configure Continuuity Reactor. It provides the 
`system <#system-requirements>`__,
`network <#network-requirements>`__, and 
`software requirements <#software-prerequisites>`__, 
`packaging options <#packaging>`__, and 
instructions for 
`installation <#installation>`__ and 
`verification <#verification>`__ of 
the Continuuity Reactor components so they work with your existing Hadoop cluster.

These are the Continuuity Reactor components:

- **Continuuity Web Cloud App**: User interface—the *Dashboard*—for managing 
  Continuuity Reactor applications;
- **Continuuity Gateway**: Service supporting REST endpoints for Continuuity Reactor; 
- **Continuuity AppFabric**: Service for managing runtime, lifecycle and resources of
  Reactor applications;
- **Continuuity DataFabric**: Service for managing data operations;
- **Continuuity Watchdog**: Metrics and logging service; and
- **Continuuity Kafka**: Metrics and logging transport service, using an embedded version of *Kafka*.
 
Before installing the Continuuity Reactor components, you must first install a Hadoop cluster with *HDFS*, *YARN*, *HBase*, and *Zookeeper*. All Reactor components can be installed on the same boxes as your Hadoop cluster, or on separate boxes that can connect to the Hadoop services. 

Our recommended installation is to use two boxes for the Reactor components; the
`hardware requirements <#hardware-requirements>`__ are relatively modest, 
as most of the work is done by the Hadoop cluster. These two
boxes provide high availability; at any one time, one of them is the leader
providing services while the other is a follower providing failover support. 

Some Reactor components run on YARN, while others orchestrate the Hadoop cluster. 
The Continuuity Gateway service starts a router instance on each of the local boxes and instantiates
one or more gateway instances on YARN as determined by the gateway service configuration.

We have specific 
`hardware <#hardware-requirements>`_, 
`network <#network-requirements>`_ and 
`prerequisite software <#software-prerequisites>`_ requirements detailed 
`below <#system-requirements>`__ 
that need to be met and completed before installation of the Continuuity Reactor components.

Conventions
-----------
In this document, *client* refers to an external application that is calling the Continuuity Reactor using the HTTP interface.

*Application* refers to a user Application that has been deployed into the Continuuity Reactor.

Text that are variables that you are to replace is indicated by a series of angle brackets (``< >``). For example::

	https://<username>:<password>@repository.continuuity.com

indicates that the texts ``<username>`` and  ``<password>`` are variables
and that you are to replace them with your values, 
perhaps username ``john_doe`` and password ``BigData11``::

	https://john_doe:BigData11@repository.continuuity.com


System Requirements
===================

Hardware Requirements
---------------------
Systems hosting the Continuuity Reactor components must meet these hardware specifications,
in addition to having CPUs with a minimum speed of 2 GHz:

.. .. list-table::
..    :widths: 20 20 60
..    :header-rows: 1
.. 
..    * - Continuuity Component
..      - Hardware Component
..      - Specifications
..    * - **Continuuity Web Cloud App**
..      - RAM
..      - 1 GB minimum, 2 GB recommended	
..    * - **Continuuity Gateway**
..      - RAM
..      - 2 GB minimum, 4 GB recommended	
..    * - **Continuuity AppFabric**
..      - RAM
..      - 2 GB minimum, 4 GB recommended	
..    * - **Continuuity DataFabric**
..      - RAM
..      - 8 GB minimum, 16 GB recommended	
..    * - **Continuuity Watchdog**
..      - RAM
..      - 2 GB minimum, 4 GB recommended	
..    * - **Continuuity Kafka**
..      - RAM
..      - 1 GB minimum, 2 GB recommended	
..    * - 
..      - Disk Space
..      - *Continuuity Kafka* maintains a data cache in a configurable data directory.
..        Required space depends on the number of Continuuity applications
..        deployed and running in the Continuuity Reactor
..        and the quantity of logs and metrics that they generate.

+-------------------------------+--------------------+-----------------------------------------------+
| Continuuity Component         | Hardware Component | Specifications                                |
+===============================+====================+===============================================+
| **Continuuity Web Cloud App** | RAM                | 1 GB minimum, 2 GB recommended                |
+-------------------------------+--------------------+-----------------------------------------------+
| **Continuuity Gateway**       | RAM                | 2 GB minimum, 4 GB recommended                |
+-------------------------------+--------------------+-----------------------------------------------+
| **Continuuity AppFabric**     | RAM                | 2 GB minimum, 4 GB recommended                |
+-------------------------------+--------------------+-----------------------------------------------+
| **Continuuity DataFabric**    | RAM                | 8 GB minimum, 16 GB recommended               |
+-------------------------------+--------------------+-----------------------------------------------+
| **Continuuity Watchdog**      | RAM                | 2 GB minimum, 4 GB recommended                |
+-------------------------------+--------------------+-----------------------------------------------+
| **Continuuity Kafka**         | RAM                | 1 GB minimum, 2 GB recommended                |
+                               +--------------------+-----------------------------------------------+
|                               | Disk Space         | *Continuuity Kafka* maintains a data cache in |
|                               |                    | a configurable data directory.                |
|                               |                    | Required space depends on the number of       |
|                               |                    | Continuuity applications deployed and running |
|                               |                    | in the Continuuity Reactor and the quantity   |
|                               |                    | of logs and metrics that they generate.       |
+-------------------------------+--------------------+-----------------------------------------------+


Network Requirements
--------------------
Continuuity components communicate over your network with *HBase*, *HDFS*, and *YARN*.
For the best performance, Continuuity components should be located on the same LAN, ideally running at 1 Gbps or faster. A good rule of thumb is to treat Continuuity components as you would *Hadoop DataNodes*.  

Software Prerequisites
----------------------
You'll need this software installed on your system:

- Java runtime
- Node.js runtime
- Hadoop/HBase environment

Java Runtime
............
The latest `JDK or JRE version 1.6.xx <http://www.java.com/en/download/manual.jsp>`__
for Linux and Solaris must be installed in your environment. 

Once you have installed the JDK, you'll need to set the JAVA_HOME environment variable.

Node.js Runtime
...............
You can download the latest version of Node.js from `nodejs.org <http://nodejs.org>`__,
using any of the methods given. 

Using Yum::

	$ curl -O http://download-i2.fedoraproject.org/pub/epel/6/i386/epel-release-6-8.noarch.rpm
	$ sudo rpm -ivh epel-release-6-8.noarch.rpm
	$ sudo yum install npm

For APT::

	$ sudo apt-get install npm
 
Hadoop/HBase Environment
........................

For a distributed enterprise, you must install these Hadoop components:

.. .. list-table::
..    :widths: 20 40 40
..    :header-rows: 1
.. 
..    * - Component
..      - Distribution
..      - Required Version
..    * - HDFS
..      - Apache Hadoop DFS,  
..      - 2.0.2-alpha or later
..    * -
..      - CDH
..      - 4.2.x or later
..    * -
..      - HDP
..      - 2.0 or later
..    * - YARN
..      - Apache Hadoop YARN
..      - 2.0.2-alpha or later
..    * -
..      - CDH
..      - 4.2.x or later
..    * -
..      - HDP
..      - 2.0 or later
..    * - HBase
..      - 
..      - 0.94.2 or later
..    * - Zookeeper
..      - 
..      - Version 3.4.3 or later	

+---------------+-------------------+------------------------+
| Component     | Distribution      | Required Version       |
+===============+===================+========================+
| **HDFS**      | Apache Hadoop DFS | 2.0.2-alpha or later   |
+               +-------------------+------------------------+
|               | CDH               | 4.2.x or later         |
+               +-------------------+------------------------+
|               | HDP               | 2.0 or later           |
+---------------+-------------------+------------------------+
| **YARN**      | Apache Hadoop DFS | 2.0.2-alpha or later   |
+               +-------------------+------------------------+
|               | CDH               | 4.2.x or later         |
+               +-------------------+------------------------+
|               | HDP               | 2.0 or later           |
+---------------+-------------------+------------------------+
| **HBase**     |                   | 0.94.2+ or 0.96.0+     |
+---------------+-------------------+------------------------+
| **Zookeeper** |                   | Version 3.4.3 or later |
+---------------+-------------------+------------------------+

Prepare the Cluster
-------------------
To prepare your cluster so that Continuuity Reactor can write to its default namespace,
create a top-level ``/continuuity`` directory in HDFS, owned by an HDFS user ``yarn``::

	hadoop fs -mkdir /continuuity && hadoop fs -chown yarn /continuuity

In the ``continuuity.com`` packages, the default HDFS namespace is ``/continuuity``
and the default HDFS user is ``yarn``. If you set up your cluster as above, no further changes are 
required.

If you want to use an HDFS directory with a name other than ``/continuuity``:

- Create the HDFS directory you want to use, such as ``/myhadoop/myspace``.
- Create an xml file ``conf/continuuity-site.xml`` (see appendix) and include in it an
  ``hdfs.namespace`` property for the HDFS directory::

	<configuration>
	 ...
	 <property>
	 <name>hdfs.namespace</name>
	 <value>/myhadoop/myspace</value>
	 <description>Default HDFS namespace</description>
	 </property>
	 ...

- Ensure that the default HDFS user ``yarn`` owns that HDFS directory.

If you want to use a different HDFS user than ``yarn``:

- Check that there is—and create if necessary—a corresponding user on all machines 
  in the cluster on which YARN is running (typically, all of the machines).
- Create an ``hdfs.user`` property for that user in ``conf/continuuity-site.xml``::

	<configuration>
	 ...
	 <property>
	 <name>hdfs.user</name>
	 <value>my_username</value>
	 <description>User for accessing HDFS</description>
	 </property>
	 ...

- Check that the HDFS user owns the HDFS directory described by ``hdfs.namespace`` on all machines.

ULIMIT Configuration
....................
When you install the Continuuity packages, the ``ulimit`` settings for the Continuuity user are specified in the ``/etc/security/limits.d/continuuity.conf`` file. On Ubuntu, they won't take effect unless you make changes to the ``/etc/pam.d/common-session file``. For more information, refer to the ``ulimit`` discussion in the `Apache HBase Reference Guide <https://hbase.apache.org/book.html#os>`__.

Packaging
=========
Continuuity components are available as either Yum ``.rpm`` or APT ``.deb`` packages. 
There is one package for each Continuuity component, and each component may have multiple
services. Additionally, there is a base Continuuity package which installs the base configuration
and the ``continuuity`` user. [**DOCNOTE: FIXME! Still true? Doesn't look like it**]
Linux support is available for *Ubuntu 12* and *CentOS 6*.

Available packaging types:

- RPM: YUM repo
- Debian: APT repo
- Tar: For specialized installations only

Continuuity packages utilize a central configuration, stored by default in ``/etc/continuuity``.

When you install the Continuuity base package, a default configuration is placed in ``/etc/continuuity/conf.dist``. The ``continuuity-site.xml`` file is a placeholder where you can define your specific configuration for all Continuuity components.

Certain Continuuity components need to reference your *Hadoop*, *HBase*, and *YARN* cluster configurations by adding them to their classpaths.

Similar to Hadoop, Continuuity utilizes the ``alternatives`` framework to allow you to easily switch between multiple configurations. The ``alternatives`` system is used for ease of
management and allows you to to choose between different directories to fulfill the 
same purpose.

Simply copy the contents of ``/etc/continuuity/conf.dist`` into a directory of your choice
(such as ``/etc/continuuity/conf.myreactor``) and make all of your customizations there. 
Then run the ``alternatives`` command to point the ``/etc/continuuity/conf`` symlink
to your custom directory. See the section 
`Install and Configure the Continuuity Base Package <url>`__ 
for more details.

RPM using Yum
-------------
Create a file ``continuuity.repo`` at the location::

	/etc/yum.repos.d/continuuity.repo

The RPM packages are accessible using Yum at this authenticated URL::

	[continuuity]
	name=Continuuity Reactor 2.1. Packages
	baseurl=https://<username>:<password>@repository.continuuity.com/content/groups/restricted
	enabled=1
	protect=0
	gpgcheck=0
	metadata_expire=30s
	autorefresh=1
	type=rpm-md

:where:
	:<username>: Username provided by your Continuuity.com representative
	:<password>: Password provided by your Continuuity.com representative

Debian using APT
----------------
Debian packages are accessible via APT on *Ubuntu 12*. 

Create a file ``continuuity.list`` at the location::

	/etc/apt/sources.list.d/continuuity.list

Use this authenticated URL (one line)::

	deb [ arch=amd64 ] https://<username>:<password>@repository.continuuity.com/content/sites/apt
            precise release

:where:
	:<username>: Username provided by your Continuuity.com representative
	:<password>: Password provided by your Continuuity.com representative


.. _installation:

Installation
============
Install the Continuuity Reactor packages by using either of these methods:

Using Yum (on one line)::

	sudo yum install continuuity-app-fabric continuuity-data-fabric continuuity-gateway 
	                 continuuity-kafka continuuity-watchdog continuuity-web-app

Using APT (on one line)::

	sudo apt-get install continuuity-app-fabric continuuity-data-fabric continuuity-gateway
	                     continuuity-kafka continuuity-watchdog continuuity-web-app

Do this on each of the boxes that are being used for the Reactor components; at a minimum,
this should be two boxes. [DOCNOTE: FIXME! Correct? What's the address then?]

This will download and install the latest version of Continuuity Reactor.
When all the packages have been installed and all the services completed starting,
the Continuuity Web Cloud App should now be accessible through a browser
at port 9999. The URL will be ``http://<app-fabric-ip>:9999`` where
``<app-fabric-ip>`` is the IP address of the machine where you installed the packages.

Verification
==========================
To verify that the Continuuity software is successfully installed, run an example application.
We provide pre-built ``.JAR`` files for convenience:

#. Download and install the latest Continuuity Developer Suite from
   http://accounts.continuuity.com.

#. Extract to a folder (``CONTINUUITY_HOME``).
#. Open a command prompt and navigate to ``CONTINUUITY_HOME/examples``.
#. Each example folder has in its ``target`` directory a .JAR file.
   For verification, we will use the ``TrafficAnalytics`` example.
#. Open a web browser to the Continuuity Reactor Dashboard (the management user interface).
   It will be located on port ``9999`` of the box where you installed the `continuuity-web-app`.
#. On the Dashboard, click the button *Load an App.*
#. Find the pre-built JAR (`TrafficAnalytics-1.0.jar`) by using the dialog box to navigate to
   ``CONTINUUITY_HOME/examples/TrafficAnalytics/target/TrafficAnalytics-1.0.jar``
#. Once the application is deployed, instructions on running the example can be found at the 
   `TrafficAnalytics example </developers/examples/TrafficAnalytics#running-the-example>`__.
#. You should be able to start the application, inject log entries,
   run the ``MapReduce`` job and see results.
#. When finished, stop and remove the application as described in the
   `TrafficAnalytics example </developers/examples/TrafficAnalytics#stopping-the-app>`__.

Troubleshooting
---------------
Here are some selected examples of potential problems and possible resolutions.

Application Won't Start
.......................
Check HDFS write permissions. It should show an obvious exception in the YARN logs.
 
Application Won't Deploy or ``logsaver`` Won't Launch YARN Container
....................................................................
If you receive an exception such as::

	AppFabricServiceException: Failed on local exception:
	com.google.protobuf.InvalidProtocolBufferException:
	Message missing required fields: callId, status; Host Details : local host is: 
 
then the Hadoop ``libs`` packaged with Continuuity are incompatible with those of
the running YARN cluster. Check the requirements for the 
`installation of the Continuuity Hadoop-Libs Package <#hadoop-libs-package>`__.
[DOCNOTE: FIXME! We took all thsi stuff out...]
 
No Metrics/logs
...............
Make sure the *Kafka* server is running, and make sure local the logs directory is created and accessible.
On the initial startup, the number of available seed brokers must be greater than or equal to the
*Kafka* default replication factor.

In a two-box setup with a replication factor of two, if one box fails to startup, 
metrics will not show up though the application will still run::

	[2013-10-10 20:48:46,160] ERROR [KafkaApi-1511941310]
	      Error while retrieving topic metadata (kafka.server.KafkaApis)
	      kafka.admin.AdministrationException:
	             replication factor: 2 larger than available brokers: 1
 
Only the First Flowlet Showing Activity
.......................................
Check that YARN has the capacity to start any of the remaining containers.
 
 
YARN Application Shows ACCEPTED For Some Time But Then Fails
............................................................
It's possible that YARN can't extract the .JARs to the ``/tmp``,
either due to a lack of disk space or permissions.
 

Where to Go Next
================
Now that you've installed Continuuity Reactor, take a look at:

- `Introduction to Continuuity Reactor <intro>`__,
  an introduction to Big Data and the Continuuity Reactor.

Appendix: The ``continuuity-site.xml``
======================================
Here are the parameters that can be defined in the ``continuuity-site.xml`` file and their default
values.

.. list-table::
   :widths: 20 20 30
   :header-rows: 1

   * - Parameter
     - Default Value
     - Description
   * - ``reactor.namespace``
     - ``continuuity``
     - Namespace for this instance of Reactor.
   * - ``thrift.max.read.buffer``
     - ``16777216``
     - Maximum read buffer size in byte used by the Thrift server.
       Value should be set to greater than the max frame sent on the RPC channel.
   * - ``zookeeper.quorum``
     - ``127.0.0.1:2181/${reactor.namespace}``
     - Address of the Zookeeper (host:port [DOCNOTE: FIXME!]
   * - ``zookeeper.session.timeout.millis``
     - ``40000``
     - Zookeeper session time out in milliseconds.
   * - ``weave.java.reserved.memory.mb``
     - ``250``
     - Reserved non-heap memory in MB for Weave container.
   * - ``weave.jvm.gc.opts``
     - .. line-block::
        ``-verbose:gc``
        ``-Xloggc:<log-dir>/gc.log``
        ``-XX:+PrintGCDetails``
        ``-XX:+PrintGCTimeStamps``
        ``-XX:+UseGCLogFileRotation``
        ``-XX:NumberOfGCLogFiles=10``
        ``-XX:GCLogFileSize=1M``
     - Java garbage collection options for all Weave containers; **<log-dir> is ?? [DOCNOTE: FIXME!]**
   * - ``weave.no.container.timeout``
     - ``120000``
     - Amount of time in milliseconds to wait for at least one container for Weave runnable.
   * - ``weave.zookeeper.namespace``
     - ``/weave``
     - Namespace prefix for Weave Zookeeper
   * - ``hdfs.lib.dir``
     - ``${hdfs.namespace}/lib``
     - Common directory in HDFS for JAR files for coprocessors.
   * - ``hdfs.namespace``
     - ``/${reactor.namespace}``
     - Namespace for files written by Reactor.
   * - ``hdfs.user``
     - ``yarn``
     - User name for accessing HDFS.
   * - ``local.data.dir``
     - ``data``
     - Data directory for local mode.
   * - ``yarn.user``
     - ``yarn``
     - User name for running applications in YARN.
   * - ``gateway.bind.address``
     - ``localhost``
     - Hostname on which the Gateway will listen (single node only).
   * - ``gateway.connection.backlog``
     - ``20000``
     - Maximum connection backlog of Gateway.
   * - ``gateway.connectors``
     - ``stream.flume``
     - Specifies the list of collectors Reactor will use.
   * - ``gateway.max.cached.events.per.stream.num``
     - ``5000``
     - Maximum number of stream events of a single stream cached before flushing.
   * - ``gateway.max.cached.stream.events.bytes``
     - ``52428800``
     - Maximum size of stream events cached before flushing.
   * - ``gateway.max.cached.stream.events.num``
     - ``10000``
     - Maximum number of stream events cached before flushing.
   * - ``gateway.stream.callback.exec.num.threads``
     - ``5``
     - Number of threads in stream events callback executor.
   * - ``gateway.stream.events.flush.interval.ms``
     - ``150``
     - Specifies the interval at which cached stream events get flushed.
   * - ``gateway.boss.threads``
     - ``1``
     - Number of Netty server boss threads.
   * - ``gateway.exec.threads``
     - ``20``
     - Number of Netty server executor threads.
   * - ``gateway.worker.threads``
     - ``10``
     - Number of Netty server worker threads.
   * - ``stream.flume.port``
     - ``10004``
     - **[DOCNOTE: FIXME!]Still relevant?**
   * - ``stream.flume.threads``
     - ``20``
     - **[DOCNOTE: FIXME!]Still relevant?**
   * - ``data.local.storage``
     - ``${local.data.dir}/ldb``
     - The database directory.
   * - ``data.local.storage.blocksize``
     - ``1024``
     - Block size in bytes.
   * - ``data.local.storage.cachesize``
     - ``104857600``
     - Cache size in bytes.
   * - ``data.tx.bind.address``
     - ``127.0.0.1``
     - Inet address for the transaction server.
   * - ``data.tx.bind.port``
     - ``15165``
     - Port number for the transaction server.
   * - ``data.tx.command.port``
     - ``15175``
     - Port number for the transaction server. **[DOCNOTE: FIXME!] Difference?**
   * - ``data.tx.client.count``
     - ``5``
     - Number of pooled instanced of the transaction.
   * - ``data.tx.client.provider``
     - ``thread-local``
     - Provider strategy for transaction clients.
   * - ``data.tx.server.io.threads``
     - ``2``
     - Number of IO threads for the transaction.
   * - ``data.tx.server.threads``
     - ``25``
     - Number of threads for the transaction.
   * - ``data.tx.snapshot.dir``
     - ``${hdfs.namespace}/tx.snapshot``
     - Directory in HDFS used to store snapshots and logs of **[DOCNOTE: FIXME!]**
   * - ``data.tx.snapshot.local.dir``
     - ``${local.data.dir}/tx.snapshot``
     - Directory on the local filesystem used to store snapshots.
   * - ``data.tx.snapshot.interval``
     - ``300``
     - Frequency in seconds at which snapshots of transaction  **[DOCNOTE: FIXME!]**
   * - ``data.tx.snapshot.retain``
     - ``10``
     - Number of transaction snapshot files to retain as
   * - ``data.tx.janitor.enable``
     - ``TRUE``
     - Whether or not the TransactionDataJanitor coprocessor is enabled.
   * - ``data.queue.config.update.interval``
     - ``5``
     - Frequency in seconds of updates to the queue consumer.
   * - ``data.queue.table.name``
     - ``queues``
     - Name of the table for queues.
   * - ``metadata.bind.address``
     - ``127.0.0.1``
     - Server address of the metadata server.
   * - ``metadata.bind.port``
     - ``45004``
     - Port of the metadata server.
   * - ``metadata.program.run.history.keepdays``
     - ``30``
     - Number of days to keep. **[DOCNOTE: FIXME! of what?]**
   * - ``log.collection.bind.address``
     - ``127.0.0.1``
     - Address of the Log Collection server.
   * - ``log.collection.bind.port``
     - ``12157``
     - Port of the Log Collection server.
   * - ``log.query.bind.address``
     - ``127.0.0.1``
     - Address of the Metrics server frontend.
   * - ``log.query.bind.port``
     - ``45002``
     - Port of the Metrics server frontend.
   * - ``log.collection.root``
     - ``${local.data.dir}/logs``
     - Root location for collecting logs
   * - ``account.server.host``
     - ``127.0.0.1``
     - Host for the account server.
   * - ``account.server.port``
     - ``8080``
     - Port for the account server.
   * - ``app.bind.address``
     - ``127.0.0.1``
     - Host address where the App-Fabric server is started.
   * - ``app.bind.port``
     - ``45000``
     - Port for the App-Fabric server. **[DOCNOTE: FIXME!]** Still relevant?
   * - ``app.command.port``
     - ``45010``
     - Command Port for the App-Fabric server. **[DOCNOTE: FIXME!]** Still relevant?
   * - ``app.output.dir``
     - ``/programs``
     - Directory where all archives are stored. **[DOCNOTE: FIXME! really?]**
   * - ``app.temp.dir``
     - ``/tmp``
     - Temp directory.
   * - ``app.program.jvm.opts``
     - ``${weave.jvm.gc.opts}``
     - Java options for all program containers
   * - ``scheduler.max.thread.pool.size``
     - ``30``
     - Size of the scheduler thread pool.
   * - ``router.bind.address``
     - ``0.0.0.0``
     - Address of the Router server.
   * - ``router.forward.rule``
     - ``10000:gateway,20000:webapp/$HOST``
     - Forward rules for Router (port:service - **[DOCNOTE: FIXME! $HOST?]**
   * - ``appfabric.environment``
     - ``devsuite``
     - Environment the appfabric is in. **[DOCNOTE: FIXME!]**
   * - ``metrics.query.bind.address``
     - ``127.0.0.1``
     - Address of the Metrics Query server.
   * - ``metrics.query.bind.port``
     - ``45005``
     - Port of the Metrics Query server.
   * - ``metrics.data.table.retention.resolution.1.seconds``
     - ``7200``
     - Retention resolution of the "1 second" table in seconds.
   * - ``metrics.kafka.partition.size``
     - ``10``
     - Number of partitions for metrics topic.
   * - ``log.publish.num.partitions``
     - ``10``
     - Number of Kafka partitions to publish the logs to.
   * - ``log.run.account``
     - ``continuuity``
     - Account to run the Logging service.
   * - ``log.base.dir``
     - ``/logs/avro``
     - Base log directory.
   * - ``log.retention.duration.days``
     - ``7``
     - Log file HDFS retention duration in days.
   * - ``log.cleanup.run.interval.mins``
     - ``1440``
     - Interval at which to run log cleanup.
   * - ``log.saver.num.instances``
     - ``1``
     - Number of log saver instances to run in YARN.
   * - ``kafka.bind.address``
     - ``0.0.0.0``
     - Hostname of Kafka server.
   * - ``kafka.bind.port``
     - ``9092``
     - Port of Kafka server.
   * - ``kafka.default.replication.factor``
     - ``1``
     - Kafka replication factor (see note below).
   * - ``kafka.log.dir``
     - ``/tmp/kafka-logs``
     - Directory to store Kafka logs.
   * - ``kafka.num.partitions``
     - ``10``
     - Default number of partitions for a topic.
   * - ``kafka.seed.brokers``
     - ``127.0.0.1:9092``
     - List of Kafka brokers (comma separated).
   * - ``kafka.zookeeper.namespace``
     - ``continuuity_kafka``
     - Zookeeper namespace for Kafka.
   * - ``enable.unrecoverable.reset``
     - ``FALSE``
     - **WARNING!**—Enabling this option enables the deletion of all applications and data.
       No recovery is possible!
   * - ``dashboard.bind.address``
     - ``0.0.0.0``
     - Address of Dashboard **[DOCNOTE: FIXME! not local host?]** Not relevant for Distributed setup?
   * - ``dashboard.bind.port``
     - ``9999``
     - Port of Dashboard **[DOCNOTE: FIXME!]** Not relevant for Distributed setup?
   * - ``gateway.server.address``
     - ``localhost``
     - Address of Gateway server **[DOCNOTE: FIXME!]** Not relevant for Distributed setup?
   * - ``gateway.server.port``
     - ``10000``
     - Port of Gateway server **[DOCNOTE: FIXME!]** Not relevant for Distributed setup?

:Note:
	``kafka.default.replication.factor`` is used to replicate *Kafka* messages across multiple machines
	to prevent data loss in the event of a hardware failure. The recommended setting is to run at least
	two *Kafka* servers. If you are running two *Kafka* servers, set this value to 2; otherwise, set it
	to the number of *Kafka* servers 
