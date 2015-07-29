.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

===============================
Sinks: Real-time: Cube
===============================

.. rubric:: Description: RealtimeSink that writes data to a Cube dataset

This RealtimeCubeSink takes a StructuredRecord in, maps it to a CubeFact, and writes it to
a Cube dataset identified by the Dataset Name property.

If Cube dataset does not exist, it will be created using properties provided with this
sink.

To configure transformation from a StructuredRecord to a CubeFact, the mapping
configuration is required, following StructuredRecordToCubeFact documentation.
