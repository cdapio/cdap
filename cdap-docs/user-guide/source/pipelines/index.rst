.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016-2017 Cask Data, Inc.

.. _pipelines-user-guide-index:

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

Data Pipelines allow you to ingest, transform, and load data, taking advantage of CDAP's ability to translate transformations and programmatic logic into parallelized computations using Spark and MapReduce. Simply, this means data pipelines makes it simple to ingest and transform large quantities of data in a short amount of time.

.. figure:: /_images/data-pipelines.jpeg
  :figwidth: 100%
  :width: 500px
  :align: center
  :class: bordered-image

Data Pipelines are represented by a series of stages arranged in a Directed Acylic Graph (DAG). This forms a one-way pipeline. Stages, which are the "nodes" in the pipeline graph, can be broadly categorized into three categories: sources, transforms, and sinks.

Sources are databases, files, or real-time streams from which you obtain your data. They enable you to ingest data, using a simple UI so you don't have to worry about coding low-level.

Transforms allow you to manipulate data once you have ingested it. For instance, you may want to "Join" the records produced by two different sources. Or, you might want to run a logistic regression on a batch source of data. CDAP provides built-in plugins for a wide variety of such use cases. Alternatively, as described in the Developer Documentation, you may want to build you own plugin.

Finally, the results of transforms need to be written to a sink. Sinks come in a wide variety of formats -- Avro, Parquet, or a RMBDS, for example -- and the connection is created from a simple UI. Data written to these sinks can then be queried from the CDAP UI or using a RESTful API. 

In total, Data Pipelines make it possible to create and deploy Big Data applications running MapReduce or Spark using only a visual interface.
