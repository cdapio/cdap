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
A `Cube dataset
<../../../reference-manual/javadocs/co/cask/cdap/api/dataset/lib/cube/package-summary.html>`__
is an implementation of an `OLAP Cube <http://en.wikipedia.org/wiki/OLAP_cube>`__ that is
pre-packaged with CDAP. Cube datasets store multidimensional facts and provide a querying
interface for the retrieval of the data. Additionally, Cube datasets allows for
exploring of the data stored in the Cube.

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

A Cube can have multiple views configured. They can be altered by updating the dataset
properties using the :ref:`Dataset RESTful API <http-restful-api-dataset-updating>`.

Here's an example of configuring a pre-aggregated view via the dataset properties:

.. image:: /_images/cube-example.png
   :width: 642 px
   :align: center

On the bottom are two Cube dataset properties that correspond to a logical view
(aggregation) that can be defined with the SQL-like statement on the top. 

In this example, the view is configured with two dimensions: ``rack`` and ``server``.
Values for both are required: the data of a CubeFact is aggregated in this view only if a
CubeFact has non-null values for both dimensions.

.. highlight:: console

In addition to configuring aggregation views, a Cube can be configured to aggregate
for multiple time resolutions based on the ``dataset.cube.resolutions`` property, which
takes a comma-separated list of resolution values in seconds, such as ``1,60,3600``
(corresponding to 1 second, 1 minute, or 1 hour resolutions)::

  dataset.cube.resolutions=1,60,3600

By default, if no ``dataset.cube.resolutions`` property is provided, a resolution of 1
second is used.

.. highlight:: java

Querying Data
=============
Querying data in Cube dataset is the most useful part of it. One can slice, dice and
drill down into the data of the Cube. Use these methods of the API to perform queries::
  
  public interface Cube extends Dataset, BatchWritable<Object, CubeFact> {
   Collection<TimeSeries> query(CubeQuery query);
   // ...
  }

To understand the ``CubeQuery`` interface, let's look at an example:

.. image:: /_images/cube-example2.png
   :width: 908 px
   :align: center

On the right is an example of how to build a Java ``CubeQuery`` corresponding to the
SQL-like statement shown on the left.

In this example, we query two measurements: ``cpu.used`` and ``disk.reads`` and use max
and sum functions to perform aggregation if needed. The query is performed on
``rack+server`` aggregated view at 1 minute resolution. The data is selected for those
records that have a rack dimension value of ``rack1`` and for the given time range. The data is
grouped by ``server`` values and each resulting time series is limited to 100 data points.

The result of the query is a collection of ``TimeSeries``. Each timeseries corresponds to
a specific measurement and a combination of dimension values of those specified in the ``groupBy``
part::

  public final class TimeSeries {
    private final String measureName;
    private final Map<String, String> dimensionValues;
    private final List<TimeValue> timeValues;
    // ...
  }

Exploring Data
==============
Many times, in order to construct a useful query, you have to explore and discover what
data is available in the Cube. For that, Cube provides exploration APIs to search for
available dimension values and measurements in specific selection of the Cube data::

  public interface Cube extends Dataset, BatchWritable<Object, CubeFact> {
   Collection<DimensionValue> findDimensionValues(CubeExploreQuery query);
   Collection<String> findMeasureNames(CubeExploreQuery query);
   // ...
  }

The ``findDimensionValues`` method finds all dimension values that the data selection
defined by ``CubeExploreQuery`` has, in addition to those specified in the
``CubeExploreQuery`` itself. Each returned value can be added to the original
``CubeExploreQuery`` to further drill down into the Cube data.

The ``findMeasureNames`` method finds all measurements that exist in the data selection specified
within a ``CubeExploreQuery``.

``CubeExploreQuery`` is performed across all aggregation views and allows you to configure
time range, resolution, dimension values to filter by, and limit the returned results
count::

    CubeExploreQuery exploreQuery = CubeExploreQuery.builder()
      .from()
        .resolution(1, TimeUnit.MINUTES)
      .where()
        .dimension("rack", "rack1")
        .timeRange(1423370200, 1423398198)
      .limit(100)
      .build();

This query defines the data selection as 1 minute resolution aggregations that have rack
dimension with value ``rack1`` and the specified time range. It limits the number of
results to 100.


AbstractCubeHttpHandler
=======================
CDAP comes with an AbstractCubeHttpHandler that can be used to quickly add a Service in
your application that provides a RESTful API on top of your Cube dataset. It is an abstract
class with only a single method to be implemented by its subclass that returns the Cube dataset
to query in::

  protected abstract Cube getCube();

Here’s an example of an application with a Cube dataset and an HTTP Service that provides
RESTful access to it::

  public class AppWithCube extends AbstractApplication {
    static final String CUBE_NAME = "cube";
    static final String SERVICE_NAME = "service";

    @Override
    public void configure() {
      DatasetProperties props = DatasetProperties.builder()
        .add("dataset.cube.resolutions", "1,60")
        .add("dataset.cube.aggregation.agg1.dimensions", "user,action")
        .add("dataset.cube.aggregation.agg1.requiredDimensions", "user,action").build();
      createDataset(CUBE_NAME, Cube.class, props);

      addService(SERVICE_NAME, new CubeHandler());
    }

    public static final class CubeHandler extends AbstractCubeHttpHandler {
      @UseDataSet(CUBE_NAME)
      private Cube cube;

      @Override
      protected Cube getCube() {
        return cube;
      }
    }
  }

.. highlight:: json

Example of the query in JSON format::

  {
      "aggregation": "rack+server",
      "startTs": 1423370200,
      "endTs":   1423398198,
      "measurements": {"cpu.used": "MAX", "disk.reads": "SUM"},
      "resolution": 60,
      "dimensionValues": {"rack": "rack1"},
      "groupByDimensions": ["server"],
      "limit": 100
  }

Example of the response in JSON format (pretty-printed to fit)::


  [
      {
          "measureName": "disk.reads",
          "dimensionValues": {
              "server": "server1"
          },
          "timeValues": [
              {
                  "timestamp": 1423370200,
                  "value": 969
              },
              {
                  "timestamp": 1423370260,
                  "value": 360
              }
          ]
      },
      {
          "measureName": "disk.reads",
          "dimensionValues": {
              "server": "server2"
          },
          "timeValues": [
              {
                  "timestamp": 1423370200,
                  "value": 23
              },
              {
                  "timestamp": 1423370260,
                  "value": 444
              }
          ]
      },
      {
          "measureName": "cpu.used",
          "dimensionValues": {
              "server": "server1"
          },
          "timeValues": [
              {
                  "timestamp": 1423370200,
                  "value": 50
              },
              {
                  "timestamp": 1423370260,
                  "value": 55
              }
          ]
      },
      {
          "measureName": "cpu.used",
          "dimensionValues": {
              "server": "server2"
          },
          "timeValues": [
              {
                  "timestamp": 1423370200,
                  "value": 12
              },
              {
                  "timestamp": 1423370260,
                  "value": 56
              }
          ]
      }
  ]



.. rubric::  Examples of Using Cube Dataset

An example of using a Cube Dataset is included in the CDAP Guide :ref:`Data Analysis with
OLAP Cube <cdap-cube-guide>`.
