.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

==================
Sinks: Batch: Cube 
==================

.. rubric:: Description: Batch Sink that writes to a Cube dataset

This BatchCubeSink takes a StructuredRecord in, maps it to a CubeFact, and writes it to
the Cube dataset identified by the name property.

If Cube dataset does not exist, it will be created using properties provided with this
sink.

To configure transformation from a StructuredRecord to a CubeFact, the mapping
configuration is required, as following the StructuredRecordToCubeFact documentation.

