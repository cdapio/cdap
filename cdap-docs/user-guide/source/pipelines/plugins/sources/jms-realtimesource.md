# JMS


Description
-----------
Java Message Service (JMS) real-time source. Emits a record with a field 'message' of type string.


Use Case
--------
This source is used whenever you want to read from a JMS queue. For example, you may want to read
from an Apache ActiveMQ queue and write to a stream.


Properties
----------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**jms.destination.name:** Name of the destination from which to retrieve messages.

**jms.messages.receive:** Maximum number of messages that should be retrieved per poll.
The default value is 50.

**jms.factory.initial:** The fully-qualified class name of the factory class that will be used to create
an initial context. This will be passed to the JNDI initial context as ``java.naming.factory.initial``.

**jms.provider.url:** Information for the service provider URL to use. This will be passed
to the JNDI initial context as ``java.naming.provider.url``.

**jms.jndi.connectionfactory.name:** The name of the connection factory from the JNDI. The default
will be ``ConnectionFactory``.

**jms.plugin.name:** Name of the JMS plugin to use. This is the value of the 'name' key defined in the
JSON file for the JMS plugin. Defaults to ``java.naming.factory.initial``.

**jms.plugin.type:** Type of the JMS plugin to use. This is the value of the 'type' key defined in the
JSON file for the JMS plugin. Defaults to ``JMSProvider``.

**jms.plugin.custom.properties:** Provide any required custom properties as a JSON Map.


Example
-------
This example will read from an instance of Apache ActiveMQ running on port 61616 on 'localhost'.
It will poll the 'purchases' topic, reading up to 50 messages with each poll. A record is
emitted for each message read from the topic. The record consists of a single field named 'message'
containing the contents of the message:

    {
        "name": "JMS",
        "type": "realtimesource",
        "properties": {
            "jms.messages.receive": "50",
            "jms.destination.name": "purchases",
            "jms.factory.initial": "org.apache.activemq.jndi.ActiveMQInitialContextFactory",
            "jms.provider.url": "tcp://localhost:61616",
            "jms.plugin.custom.properties": "{\"topic.purchases\":\"purchases\"}"
        }
    }

---
- CDAP Pipelines Plugin Type: realtimesource
- CDAP Pipelines Version: 1.7.0
