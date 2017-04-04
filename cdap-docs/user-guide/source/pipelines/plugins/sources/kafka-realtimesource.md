# Kafka


Description
-----------
Kafka real-time source. Emits a record with the schema specified by the user. If no schema
is specified, it will emit a record with two fields: 'key' (nullable string) and 'message'
(bytes).


Use Case
--------
This source is used whenever you want to read from Kafka. For example, you may want to read messages
from Kafka and write them to a stream.


Properties
----------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**kafka.partitions:** Number of partitions.

**kafka.topic:** Topic of the messages.

**kafka.zookeeper:** The connect string location of ZooKeeper.
Either that or the list of brokers is required.

**kafka.brokers:** Comma-separated list of Kafka brokers. Either that or the ZooKeeper
quorum is required.

**kafka.initial.offset:** The initial offset for the partition. Offset values -2L and -1L
have special meanings in Kafka. Default value is ``'kafka.api.OffsetRequest.EarliestTime'`` 
(-2L); a value of -1L corresponds to ``'kafka.api.OffsetRequest.LatestTime'``.

**schema:** Optional schema for the body of Kafka events.
The schema is used in conjunction with the format to parse Kafka payloads.
Some formats (such as the 'avro' format) require schema while others do not.
The schema given is for the body of the Kafka event.

**format:** Optional format of the Kafka event. Any format supported by CDAP is supported.
For example, a value of 'csv' will attempt to parse Kafka payloads as comma-separated values.
If no format is given, Kafka message payloads will be treated as bytes, resulting in a two-field schema:
'key' of type string (which is nullable) and 'payload' of type bytes.


Example
-------
This example reads from ten partitions of the 'purchases' topic of a Kafka instance.
It connects to Kafka via a ZooKeeper instance running on 'localhost'. It then 
parses Kafka messages using the 'csv' format into records with the specified schema:

    {
        "name": "Kafka",
        "type": "realtimesource",
        "properties": {
            "kafka.partitions": 10,
            "kafka.topic": "purchases",
            "kafka.zookeeper": "localhost:2181/cdap/kafka",
            "format": "csv",
            "schema": "{
                \"type\":\"record\",
                \"name\":\"purchase\",
                \"fields\":[
                    {\"name\":\"user\",\"type\":\"string\"},
                    {\"name\":\"item\",\"type\":\"string\"},
                    {\"name\":\"count\",\"type\":\"int\"},
                    {\"name\":\"price\",\"type\":\"double\"}
                ]
            }"
        }
    }

For each Kafka message read, it will output a record with the schema:

    +================================+
    | field name  | type             |
    +================================+
    | user        | string           |
    | item        | string           |
    | count       | int              |
    | price       | double           |
    +================================+

---
- CDAP Pipelines Plugin Type: realtimesource
- CDAP Pipelines Version: 1.7.0
