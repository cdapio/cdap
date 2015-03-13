.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _preferences:

=============================================
Resource Guarantees For CDAP Programs In YARN
=============================================

CDAP Master Services (Transaction, Twill, CDAP Programs (Flows, MapReduce, Services, Workers) and Explore Queries run in YARN on default YARN queues. YARN provides capabilites for resource guarantees via Capacity schedulers and Fair Schedulers. CDAP provides capabilities to submit CDAP programs and Explore Queries to non-default YARN queues.

Configurations for submitting to non-default YARN queues can be specified in two levels
a) CDAP site configuration: ``cdap-site.xml``
Configuration parameter for queue name for Master Services, CDAP Programs and Explore queries can be specified at CDAP instance level. This gets applied for all programs and hive queries. 

.. highlight:: xml

<property>
 <name>master.services.scheduler.queue</name>
 <value>sys</value>
 <description>Scheduler queue for CDAP master services</description>
 </property>

 <property>
 <name>app.scheduler.queue</name>
 <value>app</value>
 <description>Scheduler queue for CDAP Programs and Explore Queries </description>
 </property>

b) Namespace level property: 

The queue name to submit programs and hive queries in YARN can also be specified at a Namespace level, by configuring the following property using the REST API `scheduler.queue.name`. The configuration specified at the Namespace level will override the configuration specified in `cdap-site.xml`

Note: The configuration at Namespace level can only be set for CDAP programs and Explore queries. This configuration cannot be applied for Master services, since the Master services is not tied to a namespace level

Note: If the configuration is not specified at Namespace or `cdap-site` level, then all the master services, CDAP programs and Explore queries will be submitted to default YARN queues.

