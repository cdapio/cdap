.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2016 Cask Data, Inc.

.. _admin-manual-system-requirements:

===================
System Requirements
===================

In this section, we list the specific
:ref:`hardware <admin-manual-hardware-requirements>`,
:ref:`memory, core <admin-manual-memory-core-requirements>`, and
:ref:`network requirements <admin-manual-network-requirements>`, and the
:ref:`software prerequisites <admin-manual-software-requirements>`
that need to be met and completed before installation of the CDAP components.

Complete the requirements and instructions below prior to installing the CDAP components.

.. _admin-manual-hardware-requirements:

Hardware Requirements
=====================
Systems hosting the :ref:`CDAP components <admin-manual-cdap-components>`
must meet these hardware specifications, in addition to having 
**CPUs with a minimum speed of 2 GHz**:

+----------------------------+-------------------+--------------------+-----------------------------------------+
| CDAP Component             | Package           | Hardware Component | Specifications                          |
+============================+===================+====================+=========================================+
| CDAP Master                | ``cdap-master``   | RAM                | 2 GB minimum, 4 GB recommended          |
+----------------------------+-------------------+--------------------+-----------------------------------------+
| CDAP Router                | ``cdap-gateway``  | RAM                | 2 GB minimum, 4 GB recommended          |
+----------------------------+-------------------+--------------------+-----------------------------------------+
| CDAP UI                    | ``cdap-ui``       | RAM                | 1 GB minimum, 2 GB recommended          |
+----------------------------+-------------------+--------------------+-----------------------------------------+
| CDAP Kafka                 | ``cdap-kafka``    | RAM                | 1 GB minimum, 2 GB recommended          |
+                            +-------------------+--------------------+-----------------------------------------+
|                            |                   | Disk Space         | *CDAP Kafka* maintains a data cache in  |
|                            |                   |                    | a configurable data directory.          |
|                            |                   |                    | Required space depends on the number of |
|                            |                   |                    | CDAP applications deployed and running  |
|                            |                   |                    | in CDAP and the quantity of logs and    |
|                            |                   |                    | metrics that they generate.             |
+----------------------------+-------------------+--------------------+-----------------------------------------+
| CDAP Authentication Server | ``cdap-security`` | RAM                | 1 GB minimum, 2 GB recommended          |
+----------------------------+-------------------+--------------------+-----------------------------------------+

.. _admin-manual-memory-core-requirements:

Memory and Core Requirements
============================
Memory and core requirements are governed by two sources: *CDAP* and *YARN*. 

The **default settings for CDAP** are found in the :ref:`cdap-defaults.xml
<appendix-cdap-default.xml>`, and are overridden in particular instances by the
:ref:`cdap-site.xml <appendix-cdap-site.xml>` file. These vary with each service and range
from 512 to 1024 MB and from one to two cores.

The **YARN settings will override these**; for instance, the minimum YARN container size is
determined by ``yarn.scheduler.minimum-allocation-mb``. The YARN default in Hadoop is 1024
MB, so containers will be allocated with 1024 MB, even if the CDAP settings are for 512 MB.

With these default YARN and CDAP memory settings, just starting CDAP can require having 14
to 16 CPU cores (and a total of 14 to 16 GB of memory) available to YARN.


.. _admin-manual-network-requirements:

Network Requirements
====================
CDAP components communicate over your network with *HBase*, *HDFS*, and *YARN*.
For the best performance, CDAP components should be located on the same LAN,
ideally running at 1 Gbps or faster. A good rule of thumb is to treat CDAP
components as you would *Hadoop datanodes*.

See the section below (:ref:`admin-manual-cdap-and-firewalls`) for information on
configuring CDAP with a firewall and the listening ports that are used.


.. _admin-manual-software-requirements:

Software Prerequisites
======================
You'll need this software installed:

- A :ref:`Java runtime <admin-manual-install-java-runtime>` on each CDAP node and Hadoop datanode.
- A Hadoop, HBase, Hive (and optionally Spark) environment to run against.
- To use the **ad-hoc querying capabilities of CDAP,** ensure the cluster has a compatible version of
  Hive installed. See the section on :ref:`Hadoop Compatibility <admin-manual-hadoop-compatibility-matrix>`.
- If Hive is **not** going to be installed, you will need to disable the CDAP Explore
  Service, as by default it is enabled. The installation instructions describe how to configure this.
- CDAP nodes require Hadoop and HBase client installation and configuration. 
  *Note:* No Hadoop services need actually be running.
- We recommend installing an :ref:`NTP (Network Time Protocol) <admin-manual-install-ntp>`
  daemon on all nodes of the cluster, including those with CDAP components.

.. _admin-manual-install-java-runtime:

Java Runtime
------------
The latest `JDK or JRE version 1.7.xx or 1.8.xx <http://www.java.com/en/download/manual.jsp>`__
for Linux, Windows, or Mac OS X must be installed in your environment; we recommend the Oracle JDK.

.. highlight:: console

To check the Java version installed, run the command::

  $ java -version
  
CDAP is tested with the Oracle JDKs; it may work with other JDKs such as 
`Open JDK <http://openjdk.java.net>`__, but it has not been tested with them.

Once you have installed the JDK, you'll need to set the JAVA_HOME environment variable.

.. _admin-manual-install-ntp:

NTP (Network Time Protocol)
---------------------------
- We recommend installing an NTP (`Network Time Protocol <http://www.ntp.org>`__) daemon
  on all nodes of the cluster, including those with CDAP components.
- NTP requires that port 123 be open.
- If your cluster does not have access to the internet, you can run a local version of NTP
  by `setting up a master node as an NTP server <http://www.borngeek.com/2008/04/03/using-ntp-on-a-private-network/>`__.

**Installing NTP on RPM using Yum**

#. Install the NTP service and dependencies::

    $ sudo yum install ntp ntpdate ntp-doc

#. Set the service to start at reboot::

    $ sudo chkconfig ntpd on

#. Start the NTP server. This will continuously adjust the system time from an upstream NTP server::

    $ sudo /etc/init.d/ntpd start

#. Synchronize the system clock with the ``0.pool.ntp.org`` server. You should use this command only once::

    $ sudo ntpdate -u pool.ntp.org

#. Synchronize the hardware clock (to prevent synchronization problems), unless on a virtual server::

    $ sudo hwclock --systohc
  
**Installing NTP on Debian using APT**

#. Install the NTP service and dependencies::

    $ sudo apt-get install ntp

#. Start the NTP server. This will continuously adjust the system time from an upstream NTP server::

    $ sudo service ntp start

#. Synchronize the system clock with the ``0.pool.ntp.org`` server. You should use this command only once::

    $ sudo ntpdate -u pool.ntp.org

#. Synchronize the hardware clock (to prevent synchronization problems), unless on a virtual server::

    $ sudo hwclock --systohc

**NTP Troubleshooting and Configuration**

- To check the synchronization::

    $ ntpq -p

         remote           refid      st t when poll reach   delay   offset  jitter
    ==============================================================================
    +173.44.32.10    18.26.4.105      2 u    5   64    1   78.786   -0.157   1.966
    *66.241.101.63   132.163.4.103    2 u    7   64    1   43.085    2.872   0.409
    +services.quadra 198.60.22.240    2 u    6   64    1   21.805    3.040   1.033
    -hydrogen.consta 200.98.196.212   2 u    7   64    1  114.250   16.011   0.873

- If you need to adjust the configuration (add or delete servers, use servers closer to you, etc.)::

    $ vi /etc/ntp.conf


.. _admin-manual-cdap-and-firewalls:

CDAP and Firewalls
==================
In general, your :ref:`cluster configuration <admin-manual-install-deployment-architectures>`
cannot have a firewall between the cluster and CDAP. Instead, if a firewall is used, the
cluster and certain CDAP components need to be together behind the firewall. These are the
ports which can be opened to provide external access:

**Listen Ports for External Access**

.. csv-table:: 
   :header: "Description", "Governing Configuration", "Default Value in Packages/MapR", "Default Value in Ambari/Cloudera Manager"
   :widths: 25, 25, 25, 25

   "CDAP Router listen port (HTTP RESTful)", "``router.bind.port``", "11015", "11015"
   "CDAP Router listen port (HTTP RESTful) (SSL)", "``router.ssl.bind.port``", "10443", "10443"
   "CDAP UI listen port", "``dashboard.bind.port``", "11011", "11011"
   "CDAP UI listen port (SSL)", "``dashboard.ssl.bind.port``", "9443", "9443"
   "CDAP Auth Server listen port", "``security.auth.server.bind.port``", "10009", "10009"
   "CDAP Auth Server listen port (SSL)", "``security.auth.server.ssl.bind.port``", "10010", "10010"

The exact configuration and ports required will vary depending on your use of firewalls
and your specific configuration. This diagram shows a likely scenario that you could use:

.. image:: ../../developers-manual/source/_images/arch_components_view.png
   :width: 6in
   :align: center

In this diagram, we show the CDAP Router "traversing" the firewall. Note that the CDAP UI
can be completely outside of the firewall, as it needs to talk to clients, the CDAP
Router, and the CDAP Auth Server. These two services (Router and Auth Server) need to be
accessible from the outside to users, but also must be able to connect to nodes within the
cluster. They need unrestricted client access to the cluster with the ability to establish
connections to cluster nodes, on any port that a container may choose to open.

Taking this same picture, if the firewall were moved to the left of the CDAP Router/Auth
Server, then two ports (``router.bind.port``, 11015 and
``security.auth.server.bind.port``, 10009) would need to be opened to allow access by
clients to the hosts running the CDAP Router/Auth Server. There could be another firewall
between the CDAP Router/Auth Server and the cluster, as long as it provides client access
from the CDAP Auth Server to the ZooKeeper nodes. The same is true for the CDAP Router
(access to the Zookeeper nodes), except it also needs unrestricted client access, so it
usually doesn't make sense to firewall the CDAP Router when essentially you're allowing
all traffic through.

As your configuration can vary from these descriptions, this information is intended to
guide you in understanding what the different components require in order to successfully
run CDAP rather than provide strict requirements.
