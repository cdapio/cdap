# Cube


Description
-----------
Real-time sink that writes data to a CDAP Cube dataset.

Real-time Cube sink takes in a ``StructuredRecord``, maps it to a ``CubeFact``, and writes it to
the Cube dataset identified by the name property.

If Cube dataset does not exist, it will be created using properties provided with this
sink.


Use Case
--------
A real-time Cube sink is used to write data into a Cube from data sources to allow further OLAP data analysis.
For example, you can write data from a Stream into a Cube as the data arrives to perform complex
data queries across multiple dimensions and aggregated measurements.


Properties
----------
**name:** Name of the Cube dataset. If the Cube does not already exist, one will be created.

**dataset.cube.resolutions:** Aggregation resolutions to be used if a
new Cube dataset needs to be created. See [Cube dataset configuration details] for more information.

**dataset.cube.aggregations:** Provides cube aggregation dataset properties to be used
if a new Cube dataset needs to be created; provided as a collection of aggregation-groups.
Each aggregation group is identified by a unique name and value is a collection of fields.
example, if aggregations are desired on fields ``abc`` and ``xyz``, the
property can have the key ``agg1`` and value ``abc,xyz``:
See [Cube dataset configuration details] for more information.
[Cube dataset configuration details]: http://docs.cask.co/cdap/current/en/developers-manual/building-blocks/datasets/cube.html

**dataset.cube.properties:** Provides additional cube dataset properties if needed.

**cubeFact.timestamp.field:** Name of the StructuredRecord field that contains the timestamp to be used in
the CubeFact. If not provided, the current time of the record processing will be used as the CubeFact timestamp.

**cubeFact.timestamp.format:** Format of the value of timestamp field; example: "HH:mm:ss" (used if
``cubeFact.timestamp.field`` is provided).

**cubeFact.measurements:** Measurements to be extracted from StructuredRecord to be used
in CubeFact. Supports collection of measurements and requires at least one measurement to be provided.
Each measurement has measurement-name and measurement-type. Currently supported measurement types are COUNTER, GAUGE.
For example, to use the 'price' field as a
measurement of type gauge, and the 'quantity' field as a measurement of type counter, you would add two measurements
one measurement with name `price` and type `GAUGE, second measurement with name `quantity` and type `COUNTER`.


Example
-------
This configuration specifies writing data into a Cube dataset named "myCube"; it provides
dataset properties (``dataset.*``) for creating a new dataset, if one with the given name
doesn't exist; it then configures measurements to aggregate and specifies the timestamp
format for facts being written into the Cube:

    {
        "name": "Cube",
        "type": "realtimesink",
        "properties": {
            "name": "myCube",
            "dataset.cube.resolutions": "1,60,3600",
            "dataset.cube.properties": "{
                \"dataset.cube.aggregation.byName.dimensions\": \"name\",
                \"dataset.cube.aggregation.byNameByZip.dimensions\": \"name,zip\"
            }",
            "cubeFact.timestamp.field": "ts",
            "cubeFact.timestamp.format": "MM/dd/yyyy HH:mm:ss",
            "cubeFact.measurements": "{
                \"cubeFact.measurement.price\": \"GAUGE\",
                \"cubeFact.measurement.quantity\": \"COUNTER\"
            }"
        }
    }

Once the data is there, you could run queries using an ``AbstractCubeHttpHandler``. (You'd
need to deploy an application containing a service using it.) A sample query:

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

Example result:

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

---
- CDAP Pipelines Plugin Type: realtimesink
- CDAP Pipelines Version: 1.7.0
