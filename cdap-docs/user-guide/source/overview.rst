.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide: Getting Started

====================
Introduction to CDAP
====================
The Cask Data Application Platform (CDAP) is an easy-to-use platform that makes it simple to analyze huge datasets, without the headache of writing and debugging low-level code. CDAP provides familiar UIs that make Big Data accessible to anyone.

CDAP provides two visual interfaces for working with data:

- **Data Preparation** is an interactive spreadsheet that enables you to explore and manipulate your data. Learn more about :ref:`Data Preparation <data-prep-user-guide-index>`.

- **Data Pipelines** allow to link together data sources, transformations, and sinks in a flow. Learn more about :ref:`Data Pipelines <pipelines-user-guide-index>`.

More about CDAP
---------------
Cask Data Application Platform (CDAP) is an open source framework to build and deploy data applications on Apache™ Hadoop®.

CDAP provides visual tools that help ease Data Scientists work with large datasets:

- **Data Collection**: Both Data Preparation and Data Pipelines provide simple UIs for gathering data that is stored in files, databases, or real-time streams.

- **Data Exploration**: Data Preparation allows you to view and explore your data in spreadsheet format. You can also apply simple transformations to your data before deploying in a pipeline.

- **Data Processing**: Your Data Preparation transforms and custom programmatic logic are automatically translated into Spark and MapReduce jobs when deployed in Pipelines. As a result, it simple to analyze vast quantities of data very quickly.

- **Data Storage**: CDAP uses internal datasets that provide a common visual and RESTful interfaces for accessing data. The abstraction of datasets makes it simple to work with several different data formats and database types -- for instance, Avro or RDBMS -- all in the same pipeline or program, without needing to worry about the low-level details of complex data formats. 

CDAP makes it simple for data scientists and analysts to explore their data and deploy production-ready pipelines. To get started with CDAP, download CDAP `here <https://cask.co/get-cdap/>`__ and follow this tutorial `this tutorial <https://docs.cask.co/cdap/current/en/developers-manual/getting-started/standalone/index.html#standalone-index>`__ to set up CDAP own your own machine.

Data Preparation and Pipelines
------------------------------

Data Preparation
~~~~~~~~~~~~~~~~
:ref:`Data Preparation <data-prep-user-guide-index>` is an application that allows you to view, explore, and transform a small subset of your data (100 rows) before running your logic on the entire dataset (using a MapReduce or Spark job in Pipelines). This means you can quickly apply transformations to gain an understanding of how they will affect the entire dataset.

.. figure:: /_images/data-preparation.jpeg
  :figwidth: 100%
  :width: 500px
  :align: center
  :class: bordered-image

Data Prepration allows you to connect to a variety of data sources, including RDBMS, Kafka, .csv files, .json files, and more. It is simple to ingest and parse this data into a easily-understood columnar format. 

Once the data is loaded, Data Preparation allows you to inspect the data in spreadsheet format. You can apply filters on columns to better understand the distribution of the data, inspect rows that are null, and more. 

Further, Data Preparation enables you to transform your data. Data Preparation is built for applying simple transforms that do not require more complex programmatic logic. You can join columns, replace values, drop values conditionally, and much more. Further, you can re-format your data -- for instance, from .csv to .json. 

Finally, once you have explored your data and applied desired transformations, you can click ``Create Pipeline``. This will create a Pipeline (as described in the next section) which will apply the transformations you made to the entirety of your data in a parallelized Spark or MapReduce job.

Data Pipelines
~~~~~~~~~~~~~~~~
:ref:`Data Pipelines <pipelines-user-guide-index>` allow you to ingest, transform, and load data, taking advantage of CDAP's ability to translate transformations and programmatic logic into parallelized computations using Spark and MapReduce. Simply, this means data pipelines makes it simple to ingest and transform large quantities of data in a short amount of time.

.. figure:: /_images/data-pipelines.jpeg
  :figwidth: 100%
  :width: 500px
  :align: center
  :class: bordered-image

Data Pipelines are represented by a series of stages arranged in a Directed Acylic Graph (DAG). This forms a one-way pipeline. Stages, which are the "nodes" in the pipeline graph, can be broadly categorized into three categories: sources, transforms, and sinks.

Sources are databases, files, or real-time streams from which you obtain your data. They enable you to ingest data, using a simple UI so you don't have to worry about coding low-level.

Transforms allow you to manipulate data once you have ingested it. For instance, you may want to "Join" the records produced by two different sources. Or, you might want to run a logistic regression on a batch source of data. CDAP provides built-in plugins for a wide variety of such use cases. Alternatively, as described in the Developer Documentation, `you may want to build you own plugin <cdap-pipelines-developing-pipelines>`.

Finally, the results of transforms need to be written to a sink. Sinks come in a wide variety of formats -- Avro, Parquet, or a RMBDS, for example -- and the connection is created from a simple UI. Data written to these sinks can then be queried from the CDAP UI or using a RESTful API. 

In total, Data Pipelines make it possible to create and deploy Big Data applications running MapReduce or Spark using only a visual interface.
