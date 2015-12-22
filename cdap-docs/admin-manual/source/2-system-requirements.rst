.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _admin-manual-system-requirements:

===================
System Requirements
===================

.. _admin-manual-hardware-requirements:

Hardware Requirements
=====================
Systems hosting the :ref:`CDAP components <admin-manual-cdap-components>`
must meet these hardware specifications, in addition to having 
**CPUs with a minimum speed of 2 GHz**:

+----------------------------+-------------------+--------------------+-----------------------------------------------+
| CDAP Component             | Package           | Hardware Component | Specifications                                |
+============================+===================+====================+===============================================+
| CDAP Master                | ``cdap-master``   | RAM                | 2 GB minimum, 4 GB recommended                |
+----------------------------+-------------------+--------------------+-----------------------------------------------+
| CDAP Router                | ``cdap-gateway``  | RAM                | 2 GB minimum, 4 GB recommended                |
+----------------------------+-------------------+--------------------+-----------------------------------------------+
| CDAP UI                    | ``cdap-ui``       | RAM                | 1 GB minimum, 2 GB recommended                |
+----------------------------+-------------------+--------------------+-----------------------------------------------+
| CDAP Kafka                 | ``cdap-kafka``    | RAM                | 1 GB minimum, 2 GB recommended                |
+                            +-------------------+--------------------+-----------------------------------------------+
|                            |                   | Disk Space         | *CDAP Kafka* maintains a data cache in        |
|                            |                   |                    | a configurable data directory.                |
|                            |                   |                    | Required space depends on the number of       |
|                            |                   |                    | CDAP applications deployed and running        |
|                            |                   |                    | in the CDAP and the quantity                  |
|                            |                   |                    | of logs and metrics that they generate.       |
+----------------------------+-------------------+--------------------+-----------------------------------------------+
| CDAP Authentication Server | ``cdap-security`` | RAM                | 1 GB minimum, 2 GB recommended                |
+----------------------------+-------------------+--------------------+-----------------------------------------------+

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

With the default YARN and CDAP settings of memory, having 14 to 16 CPU cores 
(and a total of 14 to 16 GB of memory) available to YARN can be required just to start.

.. _admin-manual-network-requirements:

Network Requirements
====================
CDAP components communicate over your network with *HBase*, *HDFS*, and *YARN*.
For the best performance, CDAP components should be located on the same LAN,
ideally running at 1 Gbps or faster. A good rule of thumb is to treat CDAP
components as you would *Hadoop DataNodes*.  

.. _admin-manual-software-requirements:

Software Prerequisites
======================
You'll need this software installed:

- :ref:`Java runtime <admin-manual-install-java-runtime>` (on CDAP and Hadoop DataNodes)
- :ref:`Node.js runtime <admin-manual-install-node.js>` (on CDAP nodes)
- Hadoop and HBase (and optionally Hive) environment to run against
- CDAP nodes require Hadoop and HBase client installation and configuration. 
  *Note:* No Hadoop services need be running.
- We recommend installing a :ref:`NTP (Network Time Protocol) <admin-manual-install-ntp>`
  daemon on all nodes of the cluster, including those with CDAP components


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

.. _admin-manual-install-node.js:

Node.js Runtime
---------------
You can download the appropriate version of Node.js from `nodejs.org
<http://nodejs.org>`__. We recommend any version of `Node.js <https://nodejs.org/>`__
|node-js-version|; in particular, we recommend |recommended_node_js_version|.
   
**Installing Node.js on RPM using Yum**

#. Run as root (note that running under sudo will not work)::

    # su root
    # curl --silent --location https://rpm.nodesource.com/setup | bash -
    # yum -y install nodejs

#. Check the Node.js installation and version using::

    # node --version

**Installing Node.js on Debian using APT**

#. Run as root (note that running under sudo will not work)::

    $ su root
    $ curl -sL https://deb.nodesource.com/setup_5.x | bash -
    $ apt-get install --yes nodejs

#. *Note:* If there is no root password, set one using these commands, following the prompts
   to enter a new UNIX password (which will become the password for root)::

    $ sudo usermod root -p password; sudo passwd root
 
#. Check the Node.js installation and version using::

    $ node --version

   
.. _admin-manual-install-ntp:

NTP (Network Time Protocol)
---------------------------
- We recommend installing a NTP (Network Time Protocol) daemon on all nodes of the
  cluster, including those with CDAP components.
- This should be installed on all nodes of the cluster, including those with CDAP components.
- NTP requires that port 123 be open.

**Installing NTP on RPM using Yum**

#. Synchronize the system clock with the ``0.pool.ntp.org`` server. You should use this command only once::

    # sudo ntpdate -u pool.ntp.org

#. Install the NTP service and dependencies::

    # sudo yum install ntp ntpdate ntp-doc

#. Set the service to start at reboot::

    # sudo chkconfig ntpd on

#. Start the NTP server. This will continuously adjust the system time from an upstream NTP server::

   # sudo /etc/init.d/ntpd start

#. Synchronize the hardware clock (to prevent synchronization problems)::

   # hwclock --systohc
  
**Installing NTP on Debian using APT**

#. Synchronize the system clock with the ``0.pool.ntp.org`` server. You should use this command only once::

    $ sudo ntpdate -u pool.ntp.org

#. Install the NTP service and dependencies::

    $ sudo apt-get install ntp

#. Start the NTP server. This will continuously adjust the system time from an upstream NTP server::

    $ sudo service ntp start

#. Synchronize the hardware clock (to prevent synchronization problems)::

    $ hwclock --systohc

**NTP Troubleshooting and Configuration**

- To check the synchronization::

    $ ntpq -p

         remote           refid      st t when poll reach   delay   offset  jitter
    ==============================================================================
    +173.44.32.10    18.26.4.105      2 u    5   64    1   78.786   -0.157   1.966
    *66.241.101.63   132.163.4.103    2 u    7   64    1   43.085    2.872   0.409
    +services.quadra 198.60.22.240    2 u    6   64    1   21.805    3.040   1.033
    -hydrogen.consta 200.98.196.212   2 u    7   64    1  114.250   16.011   0.873

- If you need to adjust the configuration (add or delete servers, use servers closer to you)::

    $ vi /etc/ntp.conf
