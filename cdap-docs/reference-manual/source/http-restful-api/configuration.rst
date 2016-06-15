.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2015-2016 Cask Data, Inc.

.. _http-restful-api-configuration:
.. _http-restful-api-v3-configuration:

==============================
Configuration HTTP RESTful API
==============================

.. highlight:: console

Use the CDAP Configuration HTTP RESTful API to retrieve the version of CDAP and the
configurations of CDAP and HBase. 
Configurations and their settings are described in the
:ref:`Administration Manual: Installation <installation-index>` instructions.

.. Base URL explanation
.. --------------------
.. include:: base-url.txt


.. _http-restful-api-configuration-version-cdap:

CDAP Version
============

To retrieve the version of CDAP, issue an HTTP GET request::

  GET /v3/version


.. _http-restful-api-configuration-cdap:

CDAP Configurations
===================

To retrieve all the configurations used by CDAP, issue an HTTP GET request::

  GET /v3/config/cdap?format=<type>
  
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``type``
     - Format for returned type, either ``json`` (default) or ``xml``

.. highlight:: json

The response is a string in the specified format. For example, using the default JSON
format, the output would be similar to (showing the first portion, reformatted to fit)::

  {
    "security.enabled": "false",
    "app.output.dir": "\/programs",
    "router.server.port": "10000",
    "metrics.kafka.partition.size": "10",
    "data.tx.snapshot.local.dir": "data\/tx.snapshot",
    "metrics.worker.threads": "10",
    "security.server.maxthreads": "100",
    "app.worker.threads": "10",
    "security.token.digest.keylength": "128",
    "metrics.memory.mb": "256",
    "data.tx.server.io.threads": "2",

   }

.. highlight:: console

.. _http-restful-api-configuration-hbase:

HBase Configurations
====================

To retrieve all the configurations used by HBase, issue an HTTP GET request::

  GET /v3/config/hbase?format=<type>
  
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``type``
     - Format for returned type, either ``json`` (default) or ``xml``

.. highlight:: xml

The response is a string in the specified format. For example, using the XML
format, the output would be similar to (showing the first portion, reformatted to fit)::

  <configuration>
    <property>
      <name>dfs.journalnode.rpc-address</name>
        <value>0.0.0.0:8485</value>
      <source>hdfs-default.xml</source>
    </property>
    <property>
      <name>io.storefile.bloom.block.size</name>
        <value>131072</value>
      <source>hbase-default.xml</source>
    </property>
    <property>
      <name>yarn.ipc.rpc.class</name>
        <value>org.apache.hadoop.yarn.ipc.HadoopYarnProtoRPC</value>
      <source>yarn-default.xml</source>
    </property>
    <property>
      <name>mapreduce.job.maxtaskfailures.per.tracker</name>
        <value>3</value>
      <source>mapred-default.xml</source>
    </property>
    <property>
      <name>hbase.rest.threads.min</name>
        <value>2</value>
      <source>hbase-default.xml</source>
    </property>
    ...
  </configuration>
