.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide: Getting Started

==============================
Overview of Core CDAP Concepts
==============================


What is CDAP?
-------------
Cask Data Application Platform (CDAP) is an open source framework to build and deploy data applications on Apache™ Hadoop®.

CDAP makes it simple for data scientists and analysts to explore their data and deploy production-ready pipelines. To get 
started with CDAP, download CDAP `here <https://cask.co/get-cdap/>`__ and follow this tutorial `this tutorial <https://docs.cask.co/cdap/current/en/developers-manual/getting-started/standalone/index.html#standalone-index>`__
to set up CDAP own your own machine.

CDAP provides easy methods for:

- Data Collection: A method of getting data into the system, so that it can be processed.

- Data Exploration: One of the most powerful paradigms of Big Data is the ability to collect and store data without knowing details about its structure. These details are only needed at processing time. An important step—between collecting the data and processing it—is exploration; that is, examining data with ad-hoc queries to learn about its structure and nature.

- Data Processing: After data is collected, we need to process it in various ways.

- Data Storage: The results of processing data must be stored in a persistent and durable way that allows other programs or applications to further process or analyze the data. In CDAP, data is stored in datasets using the abstraction layer provided by CDAP, and domain APIs provided by datasets. This allows different data processing paradigms to interact with the dataset in their own way; in turn, this provides the flexibility in processing that a developer is looking for.


Data Preparation versus Data Pipelines
------------------------------------------------

Data Preparation
~~~~~~~~~~~~~~~~
CDAP provides a data ingestion service that simplifies and automates the difficult and time consuming task of building,
running, and managing data pipelines. The interactive studio interface allows you to drag-and-drop various sources, 
transforms, analytics, sinks, and actions.

- Drag & drop graphical studio with various sources, transforms, analytics including machine learning algorithms, sinks, and actions
- Unified interface to preview, debug, deploy, run and manage data pipelines
- Separation of logical data pipeline vs execution environment – making it easy to run pipeline as MapReduce or Spark
- Extensible pluggable architecture for integrating with new sources, sinks and transforms for processing

Data Pipelines
~~~~~~~~~~~~~~
CDAP provides an easy and interactive way to visualize, transform, and cleanse data. It helps data scientists and data engineers 
derive new schemas and operationalize the data preparation with a few clicks.

- Interactive, self-service data prep to work with messy data
- Easily connect to external data (RDBMS, Kafka, etc.)
- Apply transformations using merge, delete, and substring operations
- Quickly visualize pattern both within and across columns
- Operationalize effortlessly into production pipeline