.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

===============================
Sources: Real-time: Kafka 
===============================

.. rubric:: Description

Kafka Realtime Source. Emits a record with two fields - 'key' (nullable string) and 'message' (bytes).

.. rubric:: Use Case

TODO: Fill me out

.. rubric:: Properties

**kafka.partitions:** Number of partitions.

**kafka.topic:** Topic of the messages.

**kafka.zookeeper:** The connect string location of Zookeeper.
Either this or the list of brokers is required.

**kafka.brokers:** Comma separated list of Kafka brokers. Either this or Zookeeper connect info is required.

**kafka.default.offset:** The default offset for the partition. Default value is kafka.api.OffsetRequest.EarliestTime.

**kafka.schema:** Optional schema for the body of Kafka events.
Schema is used in conjunction with format to parse Kafka payloads. Some formats like the avro format require schema,
while others do not. The schema given is for the body of the Kafka event.

**kafka.format:** Optional format of the Kafka event. Any format supported by CDAP is also supported.
For example, a value of 'csv' will attempt to parse Kafka payloads as comma separated values.
If no format is given, Kafka message payloads will be treated as bytes, resulting in a two field schema:
'key' of type string (which is nullable) and 'payload' of type bytes.

.. rubric:: Example

TODO: Fill me out
