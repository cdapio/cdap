.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. |hadoop-distribution| replace:: MapR

.. include:: /../target/_includes/mapr-installation.rst
  :end-before: .. _mapr-install-java-runtime:

As described in these Software Prerequisites, a configured Hadoop and HBase (plus an
optional Hive client) needs to be configured on the node(s) where CDAP will run.

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

.. _mapr-install-java-runtime:

.. include:: /../target/_includes/mapr-installation.rst
  :start-after: .. _mapr-install-java-runtime:
