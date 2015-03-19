.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. highlight:: xml

.. _resource-guarantees:

=============================================
Resource Guarantees For CDAP Programs In YARN
=============================================

CDAP Master Services (Transaction, Twill), CDAP Programs (Flows, MapReduce, Services,
Workers), and CDAP Explore Queries run in YARN on default YARN queues. 

YARN provides capabilites for resource guarantees via Capacity Schedulers and Fair
Schedulers. CDAP provides capabilities to submit CDAP Master Services, CDAP programs, and
Explore Queries to non-default YARN queues.

Configurations for submitting to non-default YARN queues can be specified
by using the CDAP site configuration ``cdap-site.xml``.

The configuration parameter of the queue name for Master Services is separate from the
parameter for CDAP Programs and Explore Queries. These are applied for all programs and
Hive queries::

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

**Note:** If the configuration is not specified in the ``cdap-site.xml`` file, then all
the Master Services, CDAP programs and Explore queries will be submitted to default YARN
queues.
