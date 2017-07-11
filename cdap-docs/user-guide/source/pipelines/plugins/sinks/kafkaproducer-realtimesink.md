# Kafka Producer


Description
-----------
Kafka producer plugin that allows you to convert a Structured Record into CSV or JSON.
Plugin has the capability to push the data to one or more Kafka topics. It can
use one of the field values from input to partition the data on topic. The producer
can also be configured to operate in either sync or async mode.


Configuration
-------------
**referenceName:** This will be used to uniquely identify this sink for lineage, annotating metadata, etc.

**brokers:** Specifies a list of brokers to connect to.

**async:** Specifies whether writing the events to broker is *Asynchronous* or *Synchronous*.

**partitionfield:** Specifies the input fields that need to be used to determine the partition id; 
the field type should be int or long.

**key:** Specifies the input field that should be used as the key for the event published into Kafka.

**topics:** Specifies a list of topics to which the event should be published to.

**format:** Specifies the format of the event published to Kafka.

---
- CDAP Pipelines Plugin Type: realtimesink
- CDAP Pipelines Version: 1.7.0
