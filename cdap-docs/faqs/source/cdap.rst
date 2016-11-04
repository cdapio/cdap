.. meta::
    :author: Cask Data, Inc.
    :description: Frequently Asked Questions about starting the Cask Data Application Platform
    :copyright: Copyright © 2015-2016 Cask Data, Inc.

:titles-only-global-toc: true

.. _faqs-cdap:

==========
FAQs: CDAP
==========

.. rubric:: Configuration: General

.. highlight:: console

.. _faq-installation-startup-memory-core-requirements:

What are the memory and core requirements for CDAP?
---------------------------------------------------
The requirements are governed by two sources: CDAP and YARN, and the requirements are
:ref:`described here <admin-manual-memory-core-requirements>`.


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


How do I use YARN with the Linux Container Executor?
----------------------------------------------------
If you have YARN configured to use ``LinuxContainerExecutor`` (see the setting for
``yarn.nodemanager.container-executor.class``):
  
- The ``cdap`` user needs to be present on all Hadoop nodes.

- When using a ``LinuxContainerExecutor``, if the UID for the ``cdap`` user is less than
  500, you will need to add the ``cdap`` user to the allowed users configuration for the
  ``LinuxContainerExecutor`` in YARN by editing the ``/etc/hadoop/conf/container-executor.cfg``
  file. Change the line for ``allowed.system.users`` to::

    allowed.system.users=cdap


Where is the CDAP CLI (Command Line Interface) in Distributed mode?
-------------------------------------------------------------------
If you've installed the ``cdap-cli`` RPM or DEB, it's located under ``/opt/cdap/cli/bin``.
If you have installed CDAP manually (without using Cloudera Manager or Apache Ambari),
you can add this location to your PATH to prevent the need for specifying the entire script every time.

**Note:** These commands will list the contents of the package ``cdap-cli``, once it has
been installed::

  $ rpm -ql cdap-cli
  $ dpkg -L cdap-cli


.. rubric:: Installation: YARN


I've followed the install instructions, yet CDAP does not start and fails verification. What next?
--------------------------------------------------------------------------------------------------
If you have followed :ref:`the installation instructions <installation-index>`, and CDAP either did not pass the 
:ref:`verification step <admin-manual-verification>` or did not startup, check:

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

  - The :ref:`CDAP HDFS User <packages-configuration-options>` (by default, ``yarn``) owns the HDFS directory (by default,  ``/cdap``).
  - The :ref:`Kafka Log directory <packages-configuration-options>` (by default, ``/data/cdap/kafka-logs``), 
    must be writable by the CDAP UNIX user.
  - The :ref:`temp directories <packages-configuration-tmp-files>` utilized by CDAP must be writable by the CDAP UNIX user.

..

- Check YARN using the YARN Resource Manager UI and see if the CDAP Master services are starting up.
  Log into the cluster at ``http://<host>:8088/cluster/apps/RUNNING``. The CDAP Master
  services should be listed under "RUNNING":
  
  .. image:: _images/yarn-rm-running.png
     :align: center
     :width: 8in
  
..

- If CDAP Master has started, query the backend by using a command (substituting for ``<host>`` as appropriate)::

    $ curl -w'\n' <host>:11015/v3/system/services/status
    
  The response should be something similar to::
  
    {"dataset.executor":"OK","metrics":"OK","transaction":"OK","appfabric":"OK","metadata.service":"OK",
     "streams":"OK","explore.service":"OK","log.saver":"OK","metrics.processor":"OK"}

..

- Check that the CDAP UI is accessible (by default, the URL will be
  ``http://<host>:11011`` where ``<host>`` is the IP address of one of the machines where you
  installed the packages and started the services).


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
  
- Ensure that the CDAP UI can connect to the CDAP Router. Check that the configured ``router.server.address`` and 
  ``router.server.port`` (default 11015) in :ref:`cdap-site.xml file <packages-configuration-options>` corresponds with where the CDAP Router is listening.


I don't see the CDAP Master service on YARN.
--------------------------------------------
- Ensure that the node where CDAP is running has a properly configured YARN client.
  Can you log into the cluster at ``http://<host>:8088`` and access the YARN Resource Manager webapp?
- Ensure :ref:`YARN has enough memory and vcore capacity <faq-installation-startup-memory-core-requirements>`.
- Is the router address properly configured (``router.server.address`` and ``router.server.port`` 
  (default 11015) in :ref:`cdap-site.xml file <packages-configuration-options>`) and the boxes using it?
- Check that the classpath used includes the YARN configuration in it.


YARN Application shows ACCEPTED for some time but then fails.
-------------------------------------------------------------
It's possible that YARN can't extract the .JARs to the ``/tmp``,
either due to a lack of disk space or permissions.


.. rubric:: Installation: General


The CDAP Master log shows permissions issues.
---------------------------------------------
Ensure that ``hdfs:///${hdfs.namespace}`` and ``hdfs:///user/${hdfs.user}`` exist and are owned by ``${hdfs.user}``.
(``hdfs.namespace`` and ``hdfs.user`` are defined in your installation's :ref:`cdap-site.xml file <packages-configuration-options>`.)

In any other cases, the error should show which directory it is attempting to access, such as::

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
    - Failed to access app.meta table co.cask.cdap.api.dataset.DatasetManagementException:
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

  $ export no_proxy="localhost,127.0.0.1"


CDAP services on distributed CDAP aren't starting up due to an exception. What should I do?
-------------------------------------------------------------------------------------------
If the CDAP services on a distributed CDAP environment wouldn't start up due to a
``java.lang.ClassNotFoundException``, you will see errors in the logs. You will find in
the logs for ``cdap-master`` under ``/var/log/cdap/master*.log`` errors such as these::

 "Exception in thread "main" java.lang.NoClassDefFoundError:
   co.cask.cdap.data.runtime.main.MasterServiceMain
     at gnu.java.lang.MainThread.run(libgcj.so.10)"

Things to check as possible solutions:

1. Check if the JDK being used is :ref:`supported by CDAP <admin-manual-install-java-runtime>`::

    $ java -version

#. Check if the CDAP user is using a :ref:`correct version of the JDK <admin-manual-install-java-runtime>`::

    $ sudo su - <cdap-user> 
    $ java -version
   
#. Run this command to see if all the CDAP classpaths are included::

    $ /opt/cdap/master/bin/cdap classpath | tr ':' '\n'
   
   Expect to see (where *<version>* is the appropriate ``hbase-compat`` version)::

    /etc/cdap/conf/
    /opt/cdap/hbase-compat-<version>/lib/*
    /opt/cdap/master/conf/
    /opt/cdap/master/lib/*

   If the classpath is incorrect (in general, the ``hbase-compat-<version>/lib/*`` and
   ``cdap/master/lib/*`` entries must precede any other paths that contain JARs or classes
   such as the HBase classpath), review the :ref:`installation instructions
   <installation-index>` and correct.


We aren't seeing any Metrics or Logs. What should we do?
--------------------------------------------------------
Check that:

- ``cdap_kafka`` is running and listening on the configured port (9092 by default);
- All nodes of the cluster can successfully connect to the ``cdap_kafka`` host/port; 
  use telnet or similar to verify connectivity;
- The *Kafka* server is running;
- The local ``kafka.logs.dir`` exists and has full permissions for the ``cdap`` user; and
- For systems with high availability (HA), on the initial startup the number of available
  seed brokers must be greater than or equal to the *Kafka* default replication factor.

In a two-box HA setup with a replication factor of two, if one box fails to startup,
metrics will not show up though the application will still run::

  [2013-10-10 20:48:46,160] ERROR [KafkaApi-1511941310]
        Error while retrieving topic metadata (kafka.server.KafkaApis)
        kafka.admin.AdministrationException:
               replication factor: 2 larger than available brokers: 1

As a last resort, ``cdap_kafka`` can be reset by stopping CDAP (including ``cdap_kafka``),
removing the Kafka ``znode`` (from within ZooKeeper use ``rmr /${cdap.namespace}/kafka``)
and restarting CDAP.


Only the first flowlet of our CDAP application is showing activity.
-------------------------------------------------------------------
Check that YARN has the capacity to start any of the remaining containers.



Log Saver Process throws an Out-of-Memory Error; the CDAP UI shows service "Not OK"
-----------------------------------------------------------------------------------
The CDAP Log Saver uses an internal buffer that may overflow and result in Out-of-Memory
Errors when applications create excessive amounts of logs. One symptom of this is that the CDAP
UI *Services Explorer* shows the ``log.saver`` service as not *OK*, in addition to seeing error
messages in the logs.

By default, the Log Saver process is limited to 1GB of memory and the buffer keeps eight buckets of events
in-memory. Each event bucket contains logs generated for one second. When it is expected that logs exceeding
these settings will be produced—for example, greater than 1GB of logs generated in eight seconds—increase
the memory allocated to the Log Saver or increase the number of Log Saver instances. If the cluster has
limited memory or containers available, you can choose instead to decrease the number of in-memory event buckets.
However, decreasing the number of in-memory buckets may lead to out-of-order log events.

In the ``cdap-site.xml``, you can:

- Increase the memory by adjusting ``log.saver.container.memory.mb``;
- Increase the number of Log Saver instances using ``log.saver.container.num.cores``; and
- Adjust the number of in-memory log buckets ``log.saver.event.max.inmemory.buckets``.

See the ``log.saver`` parameter section of the :ref:`Appendix cdap-site.xml
<appendix-cdap-site.xml>` for a list of these configuration parameters and their
values that can be adjusted.


.. rubric:: Upgrading CDAP


Can a CDAP installation be upgraded more than one version?
----------------------------------------------------------
In general, no. (The exception is an upgrade from 2.8.x to 3.0.x.)
This table lists the upgrade paths available for different CDAP versions:

+---------+---------------------+
| Version | Upgrade Directly To |
+=========+=====================+
| 3.2.x   | 3.3.x               |
+---------+---------------------+
| 3.1.x   | 3.2.x               |
+---------+---------------------+
| 3.0.x   | 3.1.x               |
+---------+---------------------+
| 2.8.x   | 3.0.x               |
+---------+---------------------+
| 2.6.3   | 2.8.2               |
+---------+---------------------+

If you are doing a new installation, we recommend using the current version of CDAP.


.. _faqs-cloudera-troubleshooting-upgrade-cdh:

I missed doing a step while upgrading; how do I fix my system?
--------------------------------------------------------------
If you miss a step in the upgrade process and something goes wrong, it's possible that the
tables will get re-enabled before the coprocessors are upgraded. This could cause the
regionservers to abort and may make it very difficult to get the cluster back to a stable
state where the tables can be disabled again and complete the upgrade process.

.. highlight:: xml

In that case, set this configuration property in ``hbase-site.xml``::

  <property>
    <name>hbase.coprocessor.abortonerror</name>
    <value>false</value>
  </property>

and restart the HBase regionservers. This will allow the regionservers to start up
despite the coprocessor version mismatch. At this point, you should be able to run through
the upgrade steps successfully. 

At the end, remove the entry for ``hbase.coprocessor.abortonerror`` in order to ensure
that data correctness is maintained.


.. rubric:: Ask the CDAP Community for assistance

.. include:: cdap-user-googlegroups.txt
