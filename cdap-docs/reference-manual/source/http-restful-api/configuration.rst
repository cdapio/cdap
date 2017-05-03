.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2015-2017 Cask Data, Inc.

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

  GET /v3/config/cdap[?format=<type>]

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``type``
     - Optional format for returned type, either ``json`` (default) or ``xml``

.. highlight:: json-ellipsis

The response is a string with the property name, its value, and the source of that value,
in the specified format. For example, using the default JSON format, the output would be
similar to (showing the first portion, reformatted to fit)::

  [
      {
          "name": "log.process.pipeline.logger.cache.expiration.ms",
          "value": "300000",
          "source": "cdap-default.xml"
      },
      {
          "name": "http.service.exec.threads",
          "value": "20",
          "source": "cdap-default.xml"
      },
      {
          "name": "flow.retry.policy.base.delay.ms",
          "value": "100",
          "source": "cdap-default.xml"
      },
      {
          "name": "dashboard.ssl.bind.port",
          "value": "9443",
          "source": "cdap-default.xml"
      },
      ...
  ]

.. highlight:: console

.. _http-restful-api-configuration-hbase:

HBase Configurations
====================

To retrieve all the configurations used by HBase, issue an HTTP GET request::

  GET /v3/config/hbase[?format=<type>]

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``type``
     - Optional format for returned type, either ``json`` (default) or ``xml``

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
