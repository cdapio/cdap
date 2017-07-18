.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016-2017 Cask Data, Inc.

.. _user-guide-pipelines-user-guide-index:

:hide-toc: true

==============
CDAP Pipelines
==============

.. toctree::
   :maxdepth: 3

    Concepts and Design <concepts-design>
    Getting Started <getting-started>
    CDAP Studio <studio>
    Creating Pipelines <creating-pipelines>
    Running Pipelines <running-pipelines>
    Plugin Management <plugin-management>
    Plugin Reference <plugins/index>

.. toctree::
    :hidden:
    
    plugins

Data Pipelines allow you to ingest, transform, and load data, taking advantage of CDAP's ability to translate transformations and programmatic logic into parallelized computations using Spark and MapReduce. Data Pipelines help users to create complex data processing workflows (both batch and realtime) using an intuitive UI. By providing easy access to logs and metrics, pipelines also offer easy ways for administrators to operationalize their data processing workflows without the need for a lot of custom tooling.

.. figure:: /_images/data-pipelines.jpeg
  :figwidth: 100%
  :width: 500px
  :align: center
  :class: bordered-image

Data Pipelines are represented by a series of stages arranged in a Directed Acylic Graph (DAG). This forms a one-way pipeline. Stages, which are the "nodes" in the pipeline graph, can be broadly categorized into six categories:

- Sources
- Transforms
- Analytics
- Actions
- Sinks
- Error Handling

**Sources** are databases, files, or real-time streams from which you obtain your data. They enable you to ingest data, using a simple UI so you don't have to worry about coding low-level connections.

**Transforms** allow you to manipulate data once you have ingested it. For example, you may clone a record. You can format JSON. You can even write custom transforms using the Javascript plugin. 

**Analytics** plugins are used to run analytics or Machine Learning tasks on data. For instance, you might want to run a logistic regression or Naive Bayes on a batch source of data. CDAP provides built-in plugins for a wide variety of such use cases. 

**Action** plugins define a custom action that is scheduled to take place during a workflow, but doesn't directly manipulate data in the workflow. For example, using the Database custom action, you can run an arbitrary database command at the end of your pipeline. Alternatively, you can trigger an action to move files within an HDFS cluster.

Additionally, when stages encounter null values, logical errors, or other sources of errors, you may use an **Error Handler** plugin to catch errors. This plugin is attached to the output of any transform or analytics plugin, where it catches errors. These errors can then be written to a database to be inspected or analyzed.

Finally, data must be written to a **Sink**. Sinks come in a wide variety of formats -- Avro, Parquet, or a RMBDS, for example -- and the connection is created from a simple UI. Data written to these sinks can then be queried from the CDAP UI or using a RESTful API. 

If a plugin you need does not exist, :ref:`you may want to build you own plugin <cdap-pipelines-developing-pipelines>` as described in the Developer Documentation.

In sum, Data Pipelines make it possible to create and deploy Big Data applications running MapReduce or Spark using only a visual interface.
