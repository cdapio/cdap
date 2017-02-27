.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016-2017 Cask Data, Inc.

:section-numbering: true

.. _admin-installation-mapr:

=====================
Installation for MapR
=====================

.. include:: ../_includes/installation/installation-steps-images.txt


Preparing the Cluster
=====================
Please review the :ref:`Software Prerequisites <admin-manual-software-requirements>`, as a
configured Hadoop, HBase, and Hive (plus an optional Spark client) `MapR Converged Data
Platform <https://www.mapr.com>`__ cluster needs to be available for the node(s) where CDAP
will run.

If you are installing CDAP with the intention of using *replication,* see these
instructions on :ref:`CDAP Replication <installation-replication>` *before* installing or starting CDAP.

If colocating CDAP on cluster hosts with actual services, such as the *MapR CLDB*, *YARN
ResourceManager*, or *HBase Master*, then the client configurations will already be in place.

- To configure a MapR client, see the MapR documentation on `Setting Up the Client
  <http://doc.mapr.com/display/MapR/Setting+Up+the+Client>`__.

- To configure a MapR HBase client, see the MapR documentation on `Installing HBase on a Client
  <http://doc.mapr.com/display/MapR/Installing+HBase#InstallingHBase-HBaseonaClientInstallingHBaseonaClient>`__.

- To configure a MapR Hive client, see the MapR documentation on `Installing Hive
  <http://doc.mapr.com/display/MapR/Installing+Hive>`__.

A typical client node should have the ``mapr-client``, ``mapr-hbase``, and ``mapr-hive``
packages installed, and can be configured using the MapR `configure.sh
<http://doc.mapr.com/display/MapR/configure.sh>`__ utility.
 
.. Hadoop Configuration
.. --------------------
.. include:: ../_includes/installation/hadoop-configuration.txt

.. HDFS Permissions
.. ----------------
.. include:: /../target/_includes/mapr-hdfs-permissions.rst
      

Downloading and Distributing Packages
=====================================

.. _mapr-compatibility-matrix:

+------------------------------------------------+
| Supported MapR Distributions for Apache Hadoop |
+----------------+-------------------------------+
| CDAP Series    | MapR Distributions            |
+================+===============================+
| CDAP 3.4.x     | MapR 4.1, MapR 5.0, MapR 5.1  |
+----------------+-------------------------------+
| CDAP 3.3.x     | MapR 4.1, MapR 5.0, MapR 5.1  |
+----------------+-------------------------------+
| CDAP 3.2.x     | MapR 4.1, MapR 5.0            |
+----------------+-------------------------------+
| CDAP 3.1.x     | MapR 4.1                      |
+----------------+-------------------------------+

.. _mapr-compatibility-matrix-end:

Preparing Package Managers
--------------------------

.. include:: /../target/_includes/mapr-installation.rst
    :start-after: .. _mapr-preparing-package-managers:
    :end-before: .. _mapr-package-installation-title:


Installing CDAP Services
========================

.. include:: /../target/_includes/mapr-installation.rst
    :start-after: .. _mapr-package-installation-title:
    :end-before: .. _mapr-create-required-directories:

Create Required Directories
---------------------------

.. highlight:: console
   
To prepare your cluster so that CDAP can write to its default namespace,
create a top-level ``/cdap`` directory in MapRFS, owned by the MapRFS user ``cdap``::

  $ su mapr
  $ hadoop fs -mkdir -p /cdap && hadoop fs -chown cdap /cdap

In the CDAP packages, the default property ``hdfs.namespace`` is ``/cdap`` and the default property
``hdfs.user`` is ``yarn``.

Also, create a ``tx.snapshot`` subdirectory::

  $ su mapr
  $ hadoop fs -mkdir -p /cdap/tx.snapshot && hadoop fs -chown cdap /cdap/tx.snapshot

**Note:** If you have customized (or will be customizing) the property
``data.tx.snapshot.dir`` in your :ref:`CDAP configuration <appendix-cdap-site.xml>`, use
that value instead for ``/cdap/tx.snapshot``.


.. |display-distribution| replace:: MapR

.. |hdfs-user| replace:: ``cdap``

.. include:: /../target/_includes/mapr-configuration.rst
    :end-before:   .. _mapr-configuration-options-may-need:

#. Due to an issue with the version of the Kafka ZooKeeper client shipped with MapR, 
   it is necessary to disable use of the embedded Kafka in CDAP by setting these properties:
   
   .. highlight:: xml

   ::
   
     <property>
        <name>master.collect.containers.log</name>
        <value>false</value>
      </property>

     <property>
        <name>master.collect.app.containers.log.level</name>
        <value>OFF</value>
      </property>
      
   As a consequence of this setting, the container logs will not be streamed back to the
   master process log file. This issue is due to a `known Kafka issue 
   <https://issues.apache.org/jira/browse/TWILL-139?focusedCommentId=14598628>`__.   
    
#. Depending on your installation, you may need to set these properties:

.. include:: /../target/_includes/mapr-configuration.rst
    :start-after:   .. _mapr-configuration-options-may-need2:
    :end-before: .. _mapr-configuration-hdp:

.. highlight:: xml

YARN Application Classpath
--------------------------
CDAP requires that an additional entry |---| ``/opt/mapr/lib/*`` |---| be appended to the
``yarn.application.classpath`` setting of ``yarn-site.xml``. (This file is usually in 
``/data/mapr/hadoop/hadoop-<hadoop-version>/etc/hadoop/yarn-site.xml``.) The default
``yarn.application.classpath`` for Linux with this additional entry appended is
(reformatted to fit)::

  $HADOOP_CONF_DIR, 
  $HADOOP_COMMON_HOME/share/hadoop/common/*, 
  $HADOOP_COMMON_HOME/share/hadoop/common/lib/*, 
  $HADOOP_HDFS_HOME/share/hadoop/hdfs/*, 
  $HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*, 
  $HADOOP_YARN_HOME/share/hadoop/yarn/*, 
  $HADOOP_YARN_HOME/share/hadoop/yarn/lib/*, 
  $HADOOP_COMMON_HOME/share/hadoop/mapreduce/*, 
  $HADOOP_COMMON_HOME/share/hadoop/mapreduce/lib/*, 
  /opt/mapr/lib/*
    
**Notes:** 
   
- Since MapR might not dereference the Hadoop variables (such as ``$HADOOP_CONF_DIR``)
  correctly, we recommend specifying their full paths instead of the variables we have
  included here.

- MapR does not, by default, provide a configured ``yarn.application.classpath``, and
  you will need to add this entry to ``yarn-site.xml``. If you install using `Chef
  <https://www.getchef.com>`__, that file and entry is created automatically, but not
  with dereferenced Hadoop variables.


.. Starting CDAP Services
.. ======================

.. include:: /../target/_includes/mapr-starting.rst

.. _mapr-verification:


Verification
============

.. include:: /_includes/installation/smoke-test-cdap.txt


.. _mapr-installation-advanced-topics:

Advanced Topics
===============

- :ref:`Enabling Security <mapr-configuration-security>`
- :ref:`Enabling Kerberos <mapr-configuration-enabling-kerberos>`
- :ref:`Enabling CDAP High Availability <mapr-configuration-highly-available>`
- :ref:`Enabling Hive Execution Engines <mapr-configuration-enabling-hive-execution-engines>`

.. _mapr-configuration-security:

.. Enabling Perimeter Security
.. ---------------------------
.. include:: /../target/_includes/mapr-configuration.rst
    :start-after: .. _mapr-configuration-eps:

.. _mapr-configuration-enabling-kerberos:

.. Enabling Kerberos
.. -----------------
.. include:: /../target/_includes/mapr-configuration.rst
    :start-after: .. configuration-enabling-kerberos:
    :end-before: .. configuration-yarn-for-secure-hadoop:

..

   8. Edit ``/etc/cdap/conf/cdap-env.sh`` on each host running CDAP Master, adding::

        export OPTS="${OPTS} -Djava.security.auth.login.config=/opt/mapr/conf/mapr.login.conf -Dhadoop.login=hybrid -Dzookeeper.saslprovider=com.mapr.security.maprsasl.MaprSaslProvider"

.. include:: /../target/_includes/mapr-configuration.rst
    :start-after: .. configuration-yarn-for-secure-hadoop:
    :end-before: .. _mapr-configuration-eps:

.. Enabling CDAP HA
.. ----------------
.. include:: /../target/_includes/mapr-ha-installation.rst

.. Enabling Hive Execution Engines
.. -------------------------------
.. _mapr-configuration-enabling-hive-execution-engines:

.. include:: /_includes/installation/hive-execution-engines.txt
