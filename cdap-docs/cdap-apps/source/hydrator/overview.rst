.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _cdap-apps-etl-overview:

============
ETL Overview 
============


A Quick Intro to ETL
====================

Most data infrastructures are front ended with an ETL system (Extract-Transform-Load). The
purpose of the system is to:

- Extract data from one or more sources;
- Transform the data to fit the operational needs; and
- Load the data into a desired destination.

ETL usually comprises a source, zero or more transformations and one or more sinks, in what is called
an *ETL pipeline:*

.. image:: ../_images/etl-pipeline.png
   :width: 6in
   :align: center


.. rubric:: System Artifacts, ETL Applications, and ETL Plugins 

An *Artifact* is a blueprint that |---| with a configuration file |---| is used to create an *Application*.

CDAP provides system artifacts |---| ``cdap-etl-batch`` and ``cdap-etl-realtime`` |---|
which are used to create applications that perform ETL in either batch or real time, and
consist of a variety of sources, transformations, and sinks that are packaged together.

A third system artifact, ``cdap-etl-lib``, provides common resources for the other two system artifacts.

The batch sources can write to any batch sinks that are available and real-time sources can
write to any real-time sinks. Transformations work with either sinks or sources. Transformations
can use *validators* to test data and check that it follows user-specified rules.

This lists the available sources, sinks, and transformations (transforms):

.. list-table::
   :widths: 30 40 30
   :header-rows: 1

   * - Sources
     - Transformations
     - Sinks
   * - - **Batch Sources**

         - :ref:`Database <cdap-apps-etl-plugins-batch-sources-database>`
         - :ref:`File <cdap-apps-etl-plugins-batch-sources-file>`
         - :ref:`KVTable <cdap-apps-etl-plugins-batch-sources-kvtable>`
         - :ref:`Amazon S3 <cdap-apps-etl-plugins-batch-sources-s3>`
         - :ref:`Stream <cdap-apps-etl-plugins-batch-sources-stream>`
         - :ref:`Table <cdap-apps-etl-plugins-batch-sources-table>`
         - :ref:`TPFSAvro <cdap-apps-etl-plugins-batch-sources-tpfsavro>`

     - - **Transforms**

         - :ref:`LogParser <cdap-apps-etl-plugins-transformations-logparser>`
         - :ref:`Projection <cdap-apps-etl-plugins-transformations-projection>`
         - :ref:`Script <cdap-apps-etl-plugins-transformations-script>`
         - :ref:`ScriptFilter <cdap-apps-etl-plugins-transformations-scriptfilter>`
         - :ref:`StructuredRecordToGenericRecord <cdap-apps-etl-plugins-transformations-structuredrecordtogenericrecord>`
         - :ref:`Validator <cdap-apps-etl-plugins-transformations-validator>`

     - - **Batch Sinks**

         - :ref:`Cube <cdap-apps-etl-plugins-batch-sinks-cube>`
         - :ref:`Database <cdap-apps-etl-plugins-batch-sinks-database>`
         - :ref:`KVTable <cdap-apps-etl-plugins-batch-sinks-kvtable>`
         - :ref:`S3Avro <cdap-apps-etl-plugins-batch-sinks-s3avro>`
         - :ref:`S3Parquet <cdap-apps-etl-plugins-batch-sinks-s3parquet>`
         - :ref:`SnapshotAvro <cdap-apps-etl-plugins-batch-sinks-snapshotavro>`
         - :ref:`SnapshotParquet <cdap-apps-etl-plugins-batch-sinks-snapshotparquet>`
         - :ref:`Table <cdap-apps-etl-plugins-batch-sinks-table>`
         - :ref:`TPFSAvro <cdap-apps-etl-plugins-batch-sinks-tpfsavro>`
         - :ref:`TPFSParquet <cdap-apps-etl-plugins-batch-sinks-tpfsparquet>`

   * - - **Real-time Sources**

         - :ref:`AmazonSQS <cdap-apps-etl-plugins-real-time-sources-amazonsqs>`
         - :ref:`DataGenerator <cdap-apps-etl-plugins-real-time-sources-datagenerator>`
         - :ref:`JMS <cdap-apps-etl-plugins-real-time-sources-jms>`
         - :ref:`Kafka <cdap-apps-etl-plugins-real-time-sources-kafka>`
         - :ref:`Twitter <cdap-apps-etl-plugins-real-time-sources-twitter>`

     - - **Transforms**

         - :ref:`LogParser <cdap-apps-etl-plugins-transformations-logparser>`
         - :ref:`Projection <cdap-apps-etl-plugins-transformations-projection>`
         - :ref:`Script <cdap-apps-etl-plugins-transformations-script>`
         - :ref:`ScriptFilter <cdap-apps-etl-plugins-transformations-scriptfilter>`
         - :ref:`StructuredRecordToGenericRecord <cdap-apps-etl-plugins-transformations-structuredrecordtogenericrecord>`
         - :ref:`Validator <cdap-apps-etl-plugins-transformations-validator>`

     - - **Real-time Sinks**

         - :ref:`Cube <cdap-apps-etl-plugins-real-time-sinks-cube>`
         - :ref:`Stream <cdap-apps-etl-plugins-real-time-sinks-stream>`
         - :ref:`Table <cdap-apps-etl-plugins-real-time-sinks-table>`



.. rubric:: ETL Applications

ETL applications are created by preparing a configuration that specifies the ETL artifact and
which source, transformations (transforms), and sinks are used to create the application. The
configuration can either be written as a JSON file or, in the case of the CDAP UI,
specified in-memory.

**Batch applications** can be scheduled to run periodically using a cron expression and can read
data from batch sources using a MapReduce job. The batch application then performs any
optional transformations before writing to one or more batch sinks.

**Real time applications** are designed to poll sources periodically to fetch the data, perform any
optional transformations, and then write to one or more real-time sinks.


.. rubric:: ETL Plugins

The sources, transformations, and sinks are generically called plugins. Plugins provide a
way to extend the functionality of existing artifacts. An application can be created with
existing plugins or, if the user wishes, they can write their own source, transform, and
sink plugins to add their own. You can write your own validator using functions supplied in
either the :ref:`CoreValidator <cdap-apps-etl-plugins-shared-core-validator>` or implement your own.


Application and Plugin Details
==============================

.. |etl-creating| replace:: **Creating an ETL Application:**
.. _etl-creating: creating.html

- |etl-creating|_ Covers using the system artifacts and ETL plugins included with CDAP to create an ETL application.


.. |etl-operations| replace:: **Application Lifecycle Management:**
.. _etl-operations: ../../reference-manual/http-restful-api/lifecycle.html

- |etl-operations|_ Manage ETL Applications using CDAP's :ref:`Lifecycle HTTP RESTful API <http-restful-api-lifecycle>`.


.. |etl-custom| replace:: **Creating Custom ETL Plugins:**
.. _etl-custom: custom.html

- |etl-custom|_ Intended for developers writing custom ETL plugins.

.. |etl-plugins| replace:: **ETL Plugins:**
.. _etl-plugins: plugins/index.html

- |etl-plugins|_ Details on ETL plugins and exploring available plugins using RESTful APIs.

Upgrade Procedure
=================

If you wish to upgrade ETL applications created using the 3.2.x versions of ``cdap-etl-batch`` or ``cdap-etl-realtime``,
you can use the ETL upgrade tool packaged with the distributed version of CDAP.
The tool will connect to an instance of CDAP, look for any applications that use 3.2.x versions of the
``cdap-etl-batch`` or ``cdap-etl-realtime`` artifacts, and then update application to use the |version| version of those artifacts.
CDAP must be running when you run the command:

.. parsed-literal::
  java -cp /opt/cdap/master/libexec/cdap-etl-tools-|version|.jar co.cask.cdap.etl.tool.UpgradeTool -u http://<host>:<port> upgrade

You can also upgrade just the ETL applications within a specific namespace:

.. parsed-literal::
  java -cp /opt/cdap/master/libexec/cdap-etl-tools-|version|.jar co.cask.cdap.etl.tool.UpgradeTool -u http://<host>:<port> -n <namespace> upgrade

Or just one application:

.. parsed-literal::
  java -cp /opt/cdap/master/libexec/cdap-etl-tools-|version|.jar co.cask.cdap.etl.tool.UpgradeTool -u http://<host>:<port> -n <namespace> -p <app-name> upgrade

If you have authentication turned on, you also need to store an authentication token in a file and pass the file to the tool:

.. parsed-literal::
  java -cp /opt/cdap/master/libexec/cdap-etl-tools-|version|.jar co.cask.cdap.etl.tool.UpgradeTool -u http://<host>:<port> -a <tokenfile> upgrade
