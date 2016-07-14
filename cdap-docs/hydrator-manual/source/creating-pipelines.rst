.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _cask-hydrator-creating-pipelines:

==================
Creating Pipelines
==================

Preconfigured Pipelines
=======================

A collection of predefined and preconfigured pipelines are available from within Hydrator
Studio through the controls at the top of the left side-bar. These templates can be used
as the starting point for either your own pipelines or your own pipeline templates.

*[These names & descriptions can be extracted from cdap/cdap-ui/templates/apps/predefined/config.json]*

*[These descriptions could be expanded to much longer descriptions, with requirements for use]*


- **ETL Batch**

  - **Stream to HBase:** Periodically ingest from a stream into an HBase table
  
- **ETL Real-time**

  - **Kafka to HBase:** Ingests in real time from Kafka into an HBase table
  
  - **Kafka to Stream:** Ingests in real time from Kafka into a stream
  
  - **Kafka to OLAP Cube:** Generate an OLAP Cube in real time from Kafka
  
  - **Twitter to HBase:** Ingest real-time Twitter Stream into an HBase table
  
  - **Twitter to Stream:** Ingest real-time Twitter Stream into a stream
  
  - **Amazon SQS to HBase:** Real-time updates from Amazon Simple Queue Service into an HBase table
  
- **Data Pipeline**

  - **Model Trainer:** Train model using Naive Bayes classifier
  
  - **Event Classifier:** Classify events into spam or non-spam using a Naive Bayes model
  
  - **Log Data Aggregator:** Aggregate log data by grouping IP and HTTP Status


Creating Plugin Templates
=========================

From within Hydrator Studio, you can create a **plugin template,** a variation
of a plugin that you can configure with particular settings for re-use. 

To create a plugin-template:

- From within Hydrator Studio, hover your mouse over the plugin you would like to use
  for your template, such as the *Stream* source plugin.

- In the on-hover menu that appears, click the "+ Template" button.

- The window that appears will allow you to specify the version of the plugin to use. Once
  you do, the window will expand to allow you to specify the particular properties of that
  plugin.

- The template will require a name that uniquely identifies it. 

- You can lock individual properties of the configuration so that they are not editable
  when the template is used.

- When the plugin template is successfully saved, it will appear in with the other plugins, with
  an additional "T" icon to indicate that it is a template.

- Templates can be either edited or deleted after they are created, using buttons that
  will appear in their on-hover menu.

Once created, you can use the plugin template just as you would any other plugin, with the
advantage that it can be pre-configured with settings that you re-use or require.
