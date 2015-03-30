.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. highlight:: xml

.. _resource-guarantees:

=============================================
Resource Guarantees For CDAP Programs In YARN
=============================================

CDAP Master Services |---| Transaction, Twill, CDAP Programs (Flows, MapReduce, Services,
Workers) and Explore Queries |---| run in YARN on default YARN queues. 

YARN provides capabilites for resource guarantees via Capacity Schedulers and Fair
Schedulers. CDAP provides capabilities to submit CDAP programs and Explore Queries to
non-default YARN queues.

Configurations for submitting to non-default YARN queues can be specified in two levels.

1. Using the CDAP site configuration ``cdap-site.xml``:

   The configuration parameter of the queue name for Master Services, CDAP Programs and
   Explore Queries can be specified at the CDAP instance level. This is then applied for
   all programs and Hive queries::

    <property>
      <name>master.services.scheduler.queue</name>
      <value>sys</value>
      <description>Scheduler queue for CDAP master services</description>
    </property>

    <property>
      <name>app.scheduler.queue</name>
      <value>app</value>
      <description>Scheduler queue for CDAP Programs and Explore Queries</description>
    </property>

2. Using a Namespace-level property: 

   The queue name to submit programs and Hive queries in YARN can be specified at a
   Namespace-level by using the v3 :ref:`RESTful API <http-restful-api-namespace-editing>` to
   configure the property ``scheduler.queue.name``.
   
   The configuration specified at the Namespace-level will override the configuration
   specified in ``cdap-site.xml``.

   For example, to set ``A`` as the queue name to be used for the namespace
   *<namespace-id>*, you use this HTTP PUT method::
   
      PUT <base-url>/namespaces/<namespace-id>/properties
   
   with the property as a JSON string in the body::
   
      {"config": {"scheduler.queue.name": "A"}

    
   **Note:** The configuration at the Namespace level can only be set for CDAP programs and
   Explore queries. This configuration cannot be applied for Master Services, since
   Master Services are not tied to a namespace level.

   **Note:** If the configuration is not specified at either the Namespace or
   ``cdap-site`` levels, then all the Master Services, CDAP programs and Explore queries
   will be submitted to default YARN queues.

