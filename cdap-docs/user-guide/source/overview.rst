.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide: Getting Started

====================
Introduction to CDAP
====================
The Cask Data Application Platform (CDAP) is a Unified Integration Platform for Big Data applications. It provides simple, easy-to-use abstractions to process and analyze huge datasets, without having to write and debug low-level code, leading to rapid time-to-value. It integrates the latest Big Data technologies from data integration, discovery and data science to app development, security, operations and governance. CDAP also provides familiar UIs that make Big Data accessible to anyone.

CDAP provides two visual interfaces for working with data:

- **Data Preparation** is an interactive UI that enables you to explore, cleanse, prepare, and transform your data using simple, point-and-click transformation directives. Learn more about :ref:`Data Preparation <data-prep-user-guide-index>`.

- **Data Pipelines** allow to interactively build data pipelines by linking together data sources, transformations, and sinks. It also provides insights into logs, metrics and other management capabilities for pipelines that help to operationalize data processing in production-ready, revenue-critical environments. Learn more about :ref:`Data Pipelines <pipelines-user-guide-index>`.

More about CDAP
---------------
Cask Data Application Platform (CDAP) is an open source framework to build and deploy data applications on Apache™ Hadoop®.

CDAP provides visual tools that help ease Data Scientists work with large datasets:

- **Data Collection**: Both Data Preparation and Data Pipelines provide simple UIs for gathering data that is stored in files, databases, or real-time streams.

- **Data Exploration**: Data Preparation allows you to view and explore your data in spreadsheet format. You can also apply simple transformations to your data before deploying in a pipeline.

- **Data Processing**: Your Data Preparation transforms and custom programmatic logic are automatically translated into Spark and MapReduce jobs when deployed in Pipelines. As a result, it simple to analyze vast quantities of data very quickly.

- **Data Storage**: CDAP uses internal datasets that provide a common visual and RESTful interfaces for accessing data. The abstraction of datasets makes it simple to work with several different data formats and database types -- for instance, Avro or RDBMS -- all in the same pipeline or program, without needing to worry about the low-level details of complex data formats. 

CDAP makes it simple for data scientists and analysts to explore their data and deploy production-ready pipelines. To get started with CDAP, download CDAP `here <https://cask.co/get-cdap/>`__ and follow this tutorial `this tutorial <https://docs.cask.co/cdap/current/en/developers-manual/getting-started/standalone/index.html#standalone-index>`__ to set up CDAP own your own machine.

Advanced Users
--------------
In addition to the capabilities provided by Data Preparation, Data Pipelines, and Metatada, CDAP is a full Unified Integration Platform that allows you to quickly develop and deploy custom Java applications. If you organization needs to develop a custom application, please visit :ref:`the Developer Manual <developer-index>`.