.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _cask-hydrator-advanced-preconfigured-pipelines:

=========================================
Advanced Topics: Preconfigured Pipelines
=========================================

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

