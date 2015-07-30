.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

==================
Sinks: Batch: Cube 
==================

.. rubric:: Description

A BatchCubeSink that takes a StructuredRecord in, maps it to a CubeFact, and writes it to
the Cube dataset identified by the name property.

If Cube dataset does not exist, it will be created using properties provided with this
sink.

.. rubric:: Use Case

TODO: Fill me out

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
{"cubeFact.measurement.price":"GAUGE", "cubeFact.measurement.count":"COUNTER"}.

.. rubric:: Example

TODO: Fill me out

