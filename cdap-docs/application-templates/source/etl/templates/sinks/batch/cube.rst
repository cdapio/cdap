.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

==================
Sinks: Batch: Cube 
==================

.. rubric:: Description

Batch sink that writes to a Cube dataset.

BatchCubeSink takes a StructuredRecord in, maps it to a CubeFact, and writes it to
the Cube dataset identified by the name property.

If Cube dataset does not exist, it will be created using properties provided with this
sink.

.. rubric:: Use Case

A BatchCubeSink is used to write data into a Cube from batch-enabled data sources to allow further data analysis.
E.g. with BatchCubeSink you can periodically upload data from PartitionedFileSet into a Cube to perform complex
data queries across multiple dimensions and aggregated measurements.

.. rubric:: Properties

**name:** Name of the Cube dataset. If the Cube does not already exist, one will be created.

**dataset.cube.resolutions:** Aggregation resolutions to be used if a new Cube dataset needs to be created.
See `Cube dataset configuration details <http://docs.cask.co/cdap/current/en/developers-manual/building-blocks/datasets/cube.html#cube-configuration>`__ for more information.

**dataset.cube.properties:** Provides any dataset properties to be used if a new Cube dataset
needs to be created; provided as a JSON Map. For example, if aggregations are desired on fields 'abc' and 'xyz', the
property should have the value: {"dataset.cube.aggregation.agg1.dimensions":"abc", "dataset.cube.aggregation.agg2.dimensions":"xyz"}.
See `Cube dataset configuration details <http://docs.cask.co/cdap/current/en/developers-manual/building-blocks/datasets/cube.html#cube-configuration>`__ for more information.

**cubeFact.timestamp.field:** Name of the StructuredRecord field that contains the timestamp to be used in
the CubeFact. If not provided, the current time of the record processing will be used as the CubeFact timestamp.

Name of the StructuredRecord's field that contains timestamp to be used in CubeFact.
If not provided, the current time of the record processing will be used as CubeFact timestamp.

**cubeFact.timestamp.format:** Format of the value of timestamp field; example: "HH:mm:ss" (used if
cubeFact.timestamp.field is provided).

**cubeFact.measurements:** Measurements to be extracted from StructuredRecord to be used in CubeFact.
Provide properties as a JSON Map. For example, to use the 'price' field as a measurement of type gauge,
and the 'count' field as a measurement of type counter, the property should have the value:
{"cubeFact.measurement.price":"GAUGE", "cubeFact.measurement.quantity":"COUNTER"}.

.. rubric:: Example

The following configuration tells to write data into Cube dataset with name "myCube"; provides dataset properties
(dataset.*) for creating new dataset, if the one with given name doesn't exist; configures measurements to aggregate and
how to determine timestamp for the facts to be written into Cube::

    {
        "name": "myCube",

        "dataset.cube.resolutions": "1,60,3600",
        "dataset.cube.properties": {
          "dataset.cube.aggregation.byName.dimensions": "name",
          "dataset.cube.aggregation.byNameByZip.dimensions": "name,zip",
        },

        "cubeFact.timestamp.field": "ts",
        "cubeFact.timestamp.format": "MM/dd/yyyy HH:mm:ss",
        "cubeFact.measurements": {
          "cubeFact.measurement.price": "GAUGE"
          "cubeFact.measurement.quantity": "COUNTER"
        }
    }

Once data is there, you can now run queries using AbstractCubeHttpHandler (you will need to deploy an application that
contains service using it), e.g.::

    {
        "aggregation": "byName",
        "startTs": 1323370200,
        "endTs":   1523398198,
        "measurements": {"price": "MAX"},
        "resolution": 1,
        "dimensionValues": {},
        "groupByDimensions": ["name"],
        "limit": 1000
    }

Example result::

    [
        {
            "measureName": "price",
            "dimensionValues": {
                "name": "user1"
            },
            "timeValues": [
                {
                    "timestamp": 1323370201,
                    "value": 100
                },
                {
                    "timestamp": 1323370210,
                    "value": 360
                }
            ]
        },
        {
            "measureName": "price",
            "dimensionValues": {
                "name": "user2"
            },
            "timeValues": [
                {
                    "timestamp": 1323370201,
                    "value": 200
                },
                {
                    "timestamp": 1323370210,
                    "value": 160
                }
            ]
        }
    ]
