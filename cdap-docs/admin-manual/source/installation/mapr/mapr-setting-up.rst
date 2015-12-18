.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _mapr-setting-up:

========================
MapR: Setting-up Clients
========================

As described in the :ref:`Software Prerequisites <admin-manual-software-requirements>`, 
a configured Hadoop and HBase (plus an optional Hive client) needs to be configured on the
node(s) where CDAP will run.

If colocating CDAP on cluster hosts with actual services, such as the MapR CLDB, Yarn
ResourceManager, or HBase Master, then the client configurations will already be in place.

- To configure a MapR client, see the MapR documentation on `Setting Up the Client
  <http://doc.mapr.com/display/MapR/Setting+Up+the+Client>`__.

- To configure a MapR HBase client, see the MapR documentation on `Installing HBase on a Client
  <http://doc.mapr.com/display/MapR/Installing+HBase#InstallingHBase-HBaseonaClientInstallingHBaseonaClient>`__.

- To configure a MapR Hive client, see the MapR documentation on `Installing Hive
  <http://doc.mapr.com/display/MapR/Installing+Hive>`__.

A typical client node should have the ``mapr-client``, ``mapr-hbase``, and ``mapr-hive``
packages installed, and can be configured using the MapR `configure.sh
<http://doc.mapr.com/display/MapR/configure.sh>`__ utility.


.. rubric:: Preparing the Cluster

.. _mapr-install-preparing-the-cluster:

.. highlight:: console
   
To prepare your cluster so that CDAP can write to its default namespace,
create a top-level ``/cdap`` directory in MapRFS, owned by a MapRFS user ``cdap``::

  $ sudo -u maprfs hadoop fs -mkdir /cdap 
  $ sudo -u maprfs hadoop fs -chown cdap /cdap

In the CDAP packages, the default property ``hdfs.namespace`` is ``/cdap`` and the default property
``hdfs.user`` is ``yarn``.

Also, create a ``tx.snapshot`` subdirectory::

  $ sudo -u hdfs hadoop fs -mkdir /cdap/tx.snapshot 
  $ sudo -u hdfs hadoop fs -chown yarn /cdap/tx.snapshot

**Note:** If you have customized the property ``data.tx.snapshot.dir`` in your 
:ref:`CDAP configuration <appendix-cdap-site.xml>`, use that value instead.

.. _mapr-install-preparing-the-cluster-defaults:

.. |edit-your-cdap-configuration| replace:: edit your CDAP configuration
.. _edit-your-cdap-configuration: mapr-configurations.html

Once you have downloaded and installed the packages, you'll need to |edit-your-cdap-configuration|_,
prior to starting services.
