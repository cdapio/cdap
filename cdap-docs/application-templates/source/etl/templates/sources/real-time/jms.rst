.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

===============================
Sources: Real-time: JMS
===============================

.. rubric:: Description

JMS Real-time Source. Emits a record with a field 'message' of string type.

.. rubric:: Use Case

TODO: Fill me out

.. rubric:: Properties

**jms.destination.name:** Name of the destination from which to retrieve messages.

**jms.messages.receive:** Maximum number of messages that should be retrieved per poll.
The default value is 50.

**jms.factory.initial:** The fully-qualified class name of the factory class that will be used to create
an initial context. This will be passed to the JNDI initial context as 'java.naming.factory.initial'.

**jms.provider.url:** Information for the service provider URL to use. This will be passed
to the JNDI initial context as 'java.naming.provider.url'.

**jms.jndi.connectionfactory.name:** The name of the connection factory from the JNDI. The default
will be ConnectionFactory.

**jms.plugin.name:** Name of the JMS plugin to use. This is the value of the 'name' key defined in the
JSON file for the JMS plugin. Defaults to 'java.naming.factory.initial'.

**jms.plugin.type:** Type of the JMS plugin to use. This is the value of the 'type' key defined in the
JSON file for the JMS plugin. Defaults to 'JMSProvider'.

**jms.plugin.custom.properties:** Provide any required custom properties as a JSON Map.

.. rubric:: Example

TODO: Fill me out
