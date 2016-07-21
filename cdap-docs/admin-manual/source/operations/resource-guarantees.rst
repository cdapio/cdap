.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. highlight:: xml

.. _resource-guarantees:

=============================================
Resource Guarantees for CDAP Programs in YARN
=============================================

CDAP :term:`Master Services <master services>` |---| Transaction, Twill, CDAP programs (flows, MapReduce, 
services, workers) and CDAP Explore queries |---| run in YARN on default YARN queues. 

YARN provides capabilites for resource guarantees via Capacity Schedulers and Fair
Schedulers. CDAP provides capabilities to submit CDAP programs and CDAP Explore queries to
non-default YARN queues.

Configurations for submitting to non-default YARN queues can be specified in two levels.

1. Using the CDAP site configuration ``cdap-site.xml``:

   The configuration parameter of the queue name for Master Services, CDAP programs and
   CDAP Explore queries can be specified at the CDAP instance level. This is then applied
   for all programs and Hive queries. For example::

    <property>
      <name>master.services.scheduler.queue</name>
      <value>sys</value>
      <description>Scheduler queue for CDAP Master Services</description>
    </property>

    <property>
      <name>apps.scheduler.queue</name>
      <value>app</value>
      <description>Scheduler queue for CDAP programs and CDAP Explore queries</description>
    </property>

2. Using a namespace-level property: 

   The queue name to submit programs and Hive queries in YARN can be specified at a
   namespace-level by using the v3 :ref:`RESTful API <http-restful-api-namespace-editing>` to
   configure the property ``scheduler.queue.name``.
   
   The configuration specified at the namespace-level will override the configuration
   specified in ``cdap-site.xml``.

   For example, to set ``A`` as the queue name to be used for the namespace
   *<namespace-id>*, you use this HTTP PUT method::
   
      PUT /v3/namespaces/<namespace-id>/properties
   
   with the property as a JSON string in the body::
   
      {"config": {"scheduler.queue.name": "A"}

    
   **Note:** The configuration at the namespace level can only be set for CDAP programs and
   Explore queries. This configuration cannot be applied for Master Services, since
   Master Services are not tied to a namespace level.

   **Note:** If the configuration is not specified at either the namespace or
   ``cdap-site`` levels, then all the Master Services, CDAP programs and CDAP Explore
   queries will be submitted to default YARN queues.

