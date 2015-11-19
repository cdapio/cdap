.. meta::
    :author: Cask Data, Inc.
    :description: Frequently Asked Questions about starting the Cask Data Application Platform
    :copyright: Copyright © 2015 Cask Data, Inc.

:titles-only-toc: true

.. _faqs-cdap:

==========
FAQs: CDAP
==========


CDAP installed on CDH using Cloudera Manager gives a "No parcel" error |---| what do I do?
------------------------------------------------------------------------------------------
If, when you try to start services, you receive an error in ``stderr`` such as::
       
  Error found before invoking supervisord: No parcel provided required tags: set([u'cdap'])

The error message shows that a required parcel isn't available, suggesting that you
have not completed the last step of installing a parcel, *Activation*. There are 4 steps
to installing a parcel:

- **Adding the repository** to the list of repositories searched by Cloudera Manager
- **Downloading** the parcel to the Cloudera Manager server
- **Distributing** the parcel to all the servers in the cluster
- **Activating** the parcel

Start by clicking on the parcel icon (near the top-left corner of Cloudera Manager and looks
like a gift-wrapped box) and ensure that the CDAP parcel is listed as *Active*.

A :ref:`tutorial <step-by-step-cloudera-add-service>` is available with instructions on how to install CDAP on CDH 
(`Cloudera Data Hub <http://www.cloudera.com/content/www/en-us/resources/datasheet/cdh-datasheet.html>`__) 
using `Cloudera Manager <http://www.cloudera.com/content/www/en-us/products/cloudera-manager.html>`__. 


I've followed the install instructions, yet CDAP does not start and fails verification. What next?
--------------------------------------------------------------------------------------------------
If you have followed :ref:`the installation instructions <installation-index>`, and CDAP either did not pass the 
:ref:`verification step <hadoop-verification>` or did not startup, check:

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

  - The :ref:`CDAP HDFS User <hadoop-configuration-options>` (by default, ``yarn``) owns the HDFS directory (by default,  ``/cdap``).
  - The :ref:`Kafka Log directory <hadoop-configuration-options>` (by default, ``/data/cdap/kafka-logs``), 
    must be writable by the CDAP UNIX user.
  - The :ref:`temp directories <hadoop-configuration-tmp-files>` utilized by CDAP must be writable by the CDAP UNIX user.

..

- Check YARN using the YARN Resource Manager UI and see if the CDAP Master services are starting up.
  Log into the cluster at ``http://<host>:8088/cluster/apps/RUNNING``. The CDAP Master
  services should be listed under "RUNNING":
  
  .. image:: _images/yarn-rm-running.png
     :align: center
     :width: 8in
  
..

- If CDAP Master has started, query the backend by using a command (substituting for ``<host>`` as appropriate)::

    $ curl -w'\n' <host>:10000/v3/system/services/status
    
  The response should be something similar to::
  
    {"dataset.executor":"OK","metrics":"OK","transaction":"OK","appfabric":"OK","metadata.service":"OK",
     "streams":"OK","explore.service":"OK","log.saver":"OK","metrics.processor":"OK"}

..

- Check that the CDAP UI is accessible (by default, the URL will be
  ``http://<host>:9999`` where ``<host>`` is the IP address of one of the machines where you
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
  ``router.server.port`` (default 10000) in :ref:`cdap-site.xml file <hadoop-configuration-options>` corresponds with where the CDAP Router is listening.


I don't see the CDAP Master service on YARN.
--------------------------------------------
- Ensure that the node where CDAP is running has a properly configured YARN client.
  Can you log into the cluster at ``http://<host>:8088`` and access the YARN Resource Manager webapp?
- Ensure :ref:`YARN has enough memory and vcore capacity <faq-installation-startup-memory-core-requirements>`.
- Is the router address properly configured (``router.server.address`` and ``router.server.port`` 
  (default 10000) in :ref:`cdap-site.xml file <hadoop-configuration-options>`) and the boxes using it?
- Check that the classpath used includes the YARN configuration in it.


The CDAP Master log shows permissions issues.
---------------------------------------------
Ensure that ``hdfs:///${hdfs.namespace}`` and ``hdfs:///user/${hdfs.user}`` exist and are owned by ``${hdfs.user}``.
(``hdfs.namespace`` and ``hdfs.user`` are defined in your installation's :ref:`cdap-site.xml file <hadoop-configuration-options>`.)

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


Where is the CDAP CLI (Command Line Interface) in Distributed mode?
-------------------------------------------------------------------
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
The requirements are governed by two sources: CDAP and YARN, and the requirements are
:ref:`described here <hadoop-install-hardware-memory-core-requirements>`.

Can a CDAP installation be upgraded more than one version?
----------------------------------------------------------
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


The HiveServer2 already listens on port 10000; what should I do?
----------------------------------------------------------------
By default, CDAP uses port 10000. If port 10000 is being used by another service, simply
change the ``router.bind.port`` in the ``cdap-site.xml`` to another available port. Since
in the Hadoop ecosystem, HiveServer2 defaults to 10000, we are considering changing the
router default port.

If you use CDM or Apache Ambari to install CDAP, it will detect this and run the CDAP
Router on port 11015. Another solution is to simply run the CDAP Router on a different
host than HiveServer2.

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


.. rubric:: Ask the CDAP Community for assistance

.. include:: cdap-user-googlegroups.txt
