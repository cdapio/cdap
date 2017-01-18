.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

:hide-toc: true

.. _data-exploration:

================
Data Exploration
================

.. toctree::
   :maxdepth: 1
   
    Stream Exploration <streams>
    Fileset Exploration <filesets>
    Table Exploration <tables>
    ObjectMappedTable Exploration <object-mapped-tables>
    Custom Dataset Exploration <custom-datasets>
    Hive Execution Engines <hive-execution-engines>


This section covers how you can explore data in CDAP through the use of ad-hoc SQL-like queries.
Queries can be run over streams and certain types of datasets. We refer to this as *CDAP Explore*,
or *Explore* for short.

Enabling exploration for a dataset results in the creation of a SQL table in the Explore system. The
name of this table is, by default, the same as the name of the dataset, prefixed with ``dataset_``.
For example, after creating a Table named ``results``, it can be explored with the SQL query::

  SELECT * FROM dataset_results LIMIT 5

Note that the table is only explorable if it has a schema.

The name of the Explore table can be configured by setting the dataset property ``explore.table.name``
when creating the dataset. It is recommended to use a dataset properties builder::

    // Create the "results" partitioned file set, configure it to work with MapReduce and with Explore
    createDataset("results", PartitionedFileSet.class, PartitionedFileSetProperties.builder()
      ...
      .setEnableExploreOnCreate(true)
      .setExploreTableName("results")
      .setExploreFormat("csv")
      .setExploreSchema("`date` STRING, winner STRING, loser STRING, winnerpoints INT, loserpoints INT")
      .build());

This dataset can be queried with the configured table name; that is, without the ``dataset_`` prefix::

  SELECT * FROM results LIMIT 5

Similarly, you can configure the Explore database name by setting the dataset property ``explore.table.name``
(or calling the ``setExploreDatabaseName()`` method of the dataset properties builder).

Note that if you are running a secure cluster, additional :ref:`configuration for a secure
cluster <packages-configuration-enabling-kerberos>` is required.

Exploration of data in CDAP is governed by a combination of enabling the CDAP Explore
Service and then creating datasets and streams that are explorable. The CDAP Explore
Service is enabled by a :ref:`setting in the CDAP configuration file
<appendix-cdap-default-explore-service>` (``cdap-site.xml`` file).

Datasets and streams (that were created before the Explore Service was enabled) can be
enabled for exploration by using the :ref:`Query RESTful API <http-restful-api-query-enable-disable>`. 

You can use the same Query RESTful API to disable exploration of a specific dataset or
stream. The dataset or stream will still be accessible programmatically; it just won't
respond to queries through the HTTP RESTful API or be available for exploration using the CDAP UI.

.. |stream-exploration| replace:: **Stream Exploration:**
.. _stream-exploration: streams.html

- |stream-exploration|_ **All streams can be explored**, with support for attaching read
  formats and schemas, **enabling powerful queries.** This section covers how you attach a
  format and schema to a stream.


.. |fileset-exploration| replace:: **Fileset Exploration:**
.. _fileset-exploration: filesets.html

- |fileset-exploration|_ Describes how you can make a ``FileSet``, ``PartitionedFileSet``, 
  or ``TimePartitionedFileSet`` that is **explorable.**


.. |table-exploration| replace:: **Table Exploration:**
.. _table-exploration: tables.html

- |table-exploration|_ Describes how you can make a ``Table`` that is **explorable.**


.. |object-mapped-table-exploration| replace:: **ObjectMappedTable Exploration:**
.. _object-mapped-table-exploration: object-mapped-tables.html

- |object-mapped-table-exploration|_ Describes how you can make an ``ObjectMappedTable`` that is **explorable.**


.. |custom-dataset-exploration| replace:: **Custom Dataset Exploration:**
.. _custom-dataset-exploration: custom-datasets.html

- |custom-dataset-exploration|_ Describes how you can make a **custom dataset** that is **explorable.**


.. |hive-execution-engines| replace:: **Hive Execution Engines:**
.. _hive-execution-engines: hive-execution-engines.html

- |hive-execution-engines|_ Both `Apache Spark <http://spark.apache.org/>`__ and 
  `Apache Tez <http://tez.apache.org/>`__ can be used as an **execution engine** for CDAP Explore.

Further information on data exploration can be found in our :ref:`Integrations Manual <integrations>`.
