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
Systems hosting the CDAP components must meet these hardware specifications,
in addition to having CPUs with a minimum speed of 2 GHz:

+---------------------------------------+--------------------+-----------------------------------------------+
| CDAP Component                        | Hardware Component | Specifications                                |
+=======================================+====================+===============================================+
| **CDAP UI**                           | RAM                | 1 GB minimum, 2 GB recommended                |
+---------------------------------------+--------------------+-----------------------------------------------+
| **CDAP Router**                       | RAM                | 2 GB minimum, 4 GB recommended                |
+---------------------------------------+--------------------+-----------------------------------------------+
| **CDAP Master**                       | RAM                | 2 GB minimum, 4 GB recommended                |
+---------------------------------------+--------------------+-----------------------------------------------+
| **CDAP Kafka**                        | RAM                | 1 GB minimum, 2 GB recommended                |
+                                       +--------------------+-----------------------------------------------+
|                                       | Disk Space         | *CDAP Kafka* maintains a data cache in        |
|                                       |                    | a configurable data directory.                |
|                                       |                    | Required space depends on the number of       |
|                                       |                    | CDAP applications deployed and running        |
|                                       |                    | in the CDAP and the quantity                  |
|                                       |                    | of logs and metrics that they generate.       |
+---------------------------------------+--------------------+-----------------------------------------------+
| **CDAP Authentication Server**        | RAM                | 1 GB minimum, 2 GB recommended                |
+---------------------------------------+--------------------+-----------------------------------------------+

.. _admin-manual-install-hardware-memory-core-requirements:

Memory and Core Requirements
============================
Memory and core requirements are governed by two sources: CDAP and YARN. The default
settings for CDAP are found in the :ref:`cdap-defaults.xml <appendix-cdap-default.xml>`,
and are overridden in particular instances by the :ref:`cdap-site.xml
<appendix-cdap-site.xml>` file. These vary with each service and range from 512 to 1024 MB
and from one to two cores.

The YARN settings will override these; for instance, the minimum YARN container size is
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

- Java runtime (on CDAP and Hadoop nodes)
- Node.js runtime (on CDAP nodes)
- Hadoop and HBase (and optionally Hive) environment to run against
- CDAP nodes require Hadoop and HBase client installation and configuration. 
  **Note:** No Hadoop services need to be running.

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
You can download the appropriate version of Node.js from `nodejs.org <http://nodejs.org>`__:

#. We recommend any version of `Node.js <https://nodejs.org/>`__ |node-js-version|.
#. Download the appropriate binary ``.tar.gz`` from
   `nodejs.org/download/ <http://nodejs.org/dist/>`__.

#. Extract somewhere such as ``/opt/node-[version]/``
#. Build node.js; instructions that may assist are available at
   `github <https://github.com/joyent/node/wiki/Installing-Node.js-via-package-manager>`__
#. Ensure that ``nodejs`` is in the ``$PATH``. One method is to use a symlink from the installation:
   ``ln -s /opt/node-[version]/bin/node /usr/bin/node``
   