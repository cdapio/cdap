.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _datasets-cube:

============
Cube Dataset
============

.. highlight:: java

Overview
========
A Cube dataset [link to javadoc] is an implementation of an 
`OLAP Cube <http://en.wikipedia.org/wiki/OLAP_cube>`__ that is pre-packaged with CDAP. Cube datasets
store multidimensional facts and provide a querying interface for the retrieval of the data.
Additionally, Cube datasets allows for :term:`exploring` of the data stored in the Cube.

Storing Data
============
A Cube dataset stores multidimensional ``CubeFacts`` that contain dimension values,
measurements, and an associated timestamp::

  public class CubeFact {
   public CubeFact(long timestamp) {...}
   public CubeFact addDimensionValue(String name, String value) {...}
   public CubeFact addMeasurement(String name, MeasureType type, long value) {...}
   // ...
  } 

Where:

- ``timestamp`` is an epoch in seconds
- Dimension values are key-value pairs of a dimension ``name`` and ``value``
- Measurements have a ``name``, ``type``, and ``value``

An example of a fact could be data collected by a OS monitoring tool from a server at a
specific time:

- ``timestamp=1429929000`` *(equivalent to Sat, 25 Apr 2015 02:29:59 GMT)*
- ``dimensions``
  - ``rackId="rack1"``
  - ``serverId="server0002"``
- ``measurements``
  - ``cpu.used(gauge)=60``
  - ``disk.reads(counter)=23244``

Currently, two types of measurements are supported: gauge and counter.

Writing Data
============
The Cube Dataset API provides methods to write either a single fact or multiple facts at once::

  public interface Cube extends Dataset, BatchWritable<Object, CubeFact> {
   void add(CubeFact fact);
   void add(Collection<? extends CubeFact> facts);
   // ...
  }

Cube Configuration
==================
A Cube dataset allows for querying a pre-aggregated view of the data. That view needs to
be configured before any data is written to the Cube. Currently, a view is configured with
a list of dimensions and list of required dimensions using the :ref:`Dataset properties
<custom-datasets-properties>`.

A Cube can have multiple configured views and they can be altered by updating the dataset
properties using the :ref:`Dataset RESTful API <http-restful-api-dataset-updating>`.

Here is an example:

.. image:: /_images/cube-example.png
   :width: 688 px
   :align: center

On the bottom are two Cube dataset properties that correspond to a logical view
(aggregation) that can be defined with the SQL-like statement on the top. 

In this example, the view is configured with two dimensions: ``rack`` and ``server``.
Values for both are required: the data of a CubeFact is aggregated in this view only if a
CubeFact has non-null values for both dimensions.

.. highlight:: console

In addition to configuring aggregation views, a Cube can be configured to aggregate
for multiple time resolutions based on the ``dataset.cube.resolutions`` property, which
takes a value in seconds, such as 1, 60, or 3600 (corresponding to 1 second, 1 minute, or
1 hour resolutions)::

  dataset.cube.resolutions=60

By default, if no ``dataset.cube.resolutions`` property is provided, a resolution of 1
second is used.

.. highlight:: java

