.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

:titles-only-toc: true

.. _faq-installation-startup:

======================
CDAP FAQ: CDAP Startup
======================

.. contents::
   :depth: 2
   :local:
   :backlinks: entry
   :class: faq


Startup
=======

My CDAP install on CDH using Cloudera Manager doesn't startup |---| what do I do?
---------------------------------------------------------------------------------
We have a :ref:`tutorial <step-by-step-cloudera-add-service>` with instructions on how to install CDAP on CDH 
(`Cloudera Data Hub <http://www.cloudera.com/content/www/en-us/resources/datasheet/cdh-datasheet.html>`__) 
using `Cloudera Manager <http://www.cloudera.com/content/www/en-us/products/cloudera-manager.html>`__. 

If, when you try to start services, you receive an error in ``stderr`` such as::
       
  Error found before invoking supervisord: No parcel provided required tags: set([u'cdap'])

The error message suggests that you have not completed the last step of installing a
parcel, *Activation*. There are 4 steps to installing a parcel:

- **Adding the repository** to the list of repositories searched by Cloudera Manager
- **Downloading** the parcel to the Cloudera Manager server
- **Distributing** the parcel to all the servers in the cluster
- **Activating** the parcel

Start by clicking on the parcel icon (near the top-left corner of Cloudera Manager; looks
like a gift-wrapped box), and ensure that the CDAP parcel is listed as *Active*.


I've followed the install instructions, yet CDAP does not start and fails verification. What next?
--------------------------------------------------------------------------------------------------
If you have followed :ref:`the installation instructions <install>`, and CDAP either did not pass the 
:ref:`verification step <configuration-verification>` or did not startup, check:

- Look in the CDAP logs for error messages (located in ``$CDAP_HOME/logs``)
- If you see an error such as::

    ERROR [main:c.c.c.StandaloneMain@268] - Failed to start Standalone CDAP
    java.lang.NoSuchMethodError: 
    co.cask.cdap.UserInterfaceService.getServiceName()Ljava/lang/String

  then you have probably downloaded the standalone version of CDAP, which is not intended
  to be run on Hadoop clusters. Download the appropriate distributed packages (RPM or
  Debian version) from http://cask.co/downloads.
         
- Check permissions of directories:

  - The :ref:`CDAP HDFS User <configuration-options>` (by default, ``yarn``) owns the HDFS directory (by default,  ``/cdap``).
  - The :ref:`Kafka Log directory <configuration-options>` (by default, ``/data/cdap/kafka-logs``), must be writable by the default CDAP user.
  - The :ref:`temp directories <configuration-tmp-files>` (by default, ``/tmp`` and ``/tmp/kafka-logs``) utilized by CDAP must be writable by the default CDAP user.

.. - Check :ref:`configuration troubleshooting <configuration-troubleshooting>` suggestions
   
    
.. CDAP UI shows a blank screen
.. ----------------------------
.. TBC.


The CDAP UI is showing a message "namespace cannot be found".
-------------------------------------------------------------
This is indicative that the UI cannot connect to the CDAP system service containers running in YARN.

- First, check if the CDAP Master service container shows as RUNNING in the YARN ResourceManager UI.

- If this doesn't resolve the issue, then it means the CDAP system services were unable to launch.
  Ensure :ref:`YARN has enough spare memory and vcore capacity <faq-installation-startup-memory-core-requirements>`.  
  CDAP attempts to launch between 8 and 11 containers, depending on the configuration. Check
  the master container (Application Master) logs to see if it was able to launch all containers.

- If it was able to launch all containers, then you may need to further check the launched container logs for any errors.


.. CDAP UI shows a session time out
.. --------------------------------
.. TBC.


I don't see the CDAP Master service on YARN.
--------------------------------------------
- Ensure that the node where CDAP is running has a properly configured YARN client.
- Ensure :ref:`YARN has enough memory and vcore capacity <faq-installation-startup-memory-core-requirements>`.
- Is the router address properly configured in the :ref:`cdap-site.xml file <configuration-options>` and the boxes using it?
- Check that the classpath used includes the YARN configuration in it.


My CDAP Master log shows permissions issues.
--------------------------------------------
Ensure that ``hdfs:///#{hdfs.namespace}`` and ``hdfs:///user/#{hdfs.user}`` exist and are owned by ``#{hdfs.user}``.
(``hdfs.namespace`` and ``hdfs.user`` are defined in your installations :ref:`cdap-site.xml file <configuration-options>`.)

In rare cases, until `CDAP-3817 <https://issues.cask.co/browse/CDAP-3817>`__ is resolved,
ensure ``hdfs:///#{hdfs.namespace}/tx.snapshot`` exists and is owned by ``#{hdfs.user}``. 

In any other case, the error should show which directory it is attempting to access. Don't
hesitate to ask for help at the `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`__.


My CDAP Master log shows an error about the dataset service not being found.
----------------------------------------------------------------------------
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

Where is the CDAP CLI (Command Line Interface)?
-----------------------------------------------
If you've installed the ``cdap-cli`` RPM or Deb, it's located under ``/opt/cdap/cli/bin``.
You can add this location to your PATH to prevent the need for specifying the entire script every time.

**Note:** These commands will list the contents of the package ``cdap-cli``, once it has
been installed::

  rpm -ql cdap-cli
  dpkg -L cdap-cli

.. _faq-installation-startup-memory-core-requirements:

What are the memory and core requirements for CDAP?
---------------------------------------------------
The settings are governed by two sources: CDAP and YARN. The default setting for CDAP are
found in the ``cdap-defaults.xml``, and are over-ridden in particular instances by the
``cdap-site.xml`` file. These vary with each service and range from 512 to 1024 MB and
from one to two cores.

The YARN settings will over-ride these; for instance, the minimum YARN container size is
determined by ``yarn.scheduler.minimum-allocation-mb``. The YARN default in ``HDP/Hadoop``
is 1024 MB, so containers will be allocated with 1024 MB, even if the CDAP settings are
for 512 MB.

Can a current CDAP installation be upgraded more than one version?
------------------------------------------------------------------
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

Are at least two machines really required?
------------------------------------------
The CDAP components are independently scalable, so you can install from 1 to *N* of each
component on any combination of nodes.  The primary reasons for using at least two
machines are for HA (high availability) and for ``cdap-router``'s data ingest capacity.

It is not necessary to install all components on both machines; you could install just the
CDAP UI on a third machine with other components on the second node. You can install each
component on a separate machine (or more) if you choose. The :ref:`HA [High Availability]
Environment diagram <deployment-architectures-ha>` gives just one possible
configuration.

My Hive Server2 defaults to 10000; what should I do?
----------------------------------------------------
By default, CDAP uses port 10000. If port 10000 is being used by another service, simply
change the ``router.bind.port`` in the ``cdap-site.xml`` to another available port. Since
in the Hadoop ecosystem, Hive Server2 defaults to 10000, we are considering changing the
router default port. 
       
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

Ask the CDAP Community for assistance
-------------------------------------

.. include:: cdap-user-googlegroups.txt
