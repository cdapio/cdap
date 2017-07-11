.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide: Getting Started

.. _data-prep-user-guide-index:

:hide-toc: true

==============================
Data Preparation Documentation
==============================

.. toctree::

	Concepts <concepts>
	Directives <directives/index>
	Functions <functions/index>
	Service <service/index>
    Performance <peformance>
    Exclusion and Aliasing <exclusion-and-aliasing>

Data Preparation is an application that allows you to view, explore, and transform a small subset of your data (100 rows) before running your logic on the entire dataset (using a MapReduce or Spark job in Pipelines). This means you can quickly apply transformations to gain an understanding of how they will affect the entire dataset.

.. figure:: /_images/data-preparation.jpeg
  :figwidth: 100%
  :width: 500px
  :align: center
  :class: bordered-image

Data Prepration allows you to connect to a variety of data sources, including RDBMS, Kafka, .csv files, .json files, and more. It is simple to ingest and parse this data into a easily-understood columnar format. 

Once the data is loaded, Data Preparation allows you to inspect the data in spreadsheet format. You can apply filters on columns to better understand the distribution of the data, inspect rows that are null, and more. 

Further, Data Preparation enables you to transform your data. Data Preparation is built for applying simple transforms that do not require more complex programmatic logic. You can join columns, replace values, drop values conditionally, and much more. Further, you can re-format your data -- for instance, from .csv to .json. 

Finally, once you have explored your data and applied desired transformations, you can operationalize those transformations by clicking ``Create Pipeline``. This will create a Pipeline (as described in the next section) which will apply the transformations you made to the entirety of your data in a parallelized MapReduce, Spark or Spark Streaming job.