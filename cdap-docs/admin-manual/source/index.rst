.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _admin-index:

==================================================
CDAP Administration Manual
==================================================

Covers putting CDAP into production, with **installation and configuration, security
setup,** and **operations.** Appendices describe the **XML files** used to configure
the CDAP installation and its security configuration.


.. rubric:: Installation and Configuration

.. |installation| replace:: **Installation and Configuration:**
.. _installation: installation/index.html

|installation|_ **Distribution-specific** instructions are available, plus instructions for **generic Apache Hadoop** clusters.

.. |ambari| replace:: **Apache Ambari (HDP):**
.. _ambari: installation/ambari/index.html

- |ambari|_ Installing CDAP on `HDP (Hortonworks Data Platform) <http://hortonworks.com/>`__ clusters
  managed with `Apache Ambari <https://ambari.apache.org/>`__.

.. |cloudera| replace:: **Cloudera Manager (CDH):**
.. _cloudera: installation/cloudera/index.html

- |cloudera|_ Installing CDAP on `CDH (Cloudera Data Hub) <http://www.cloudera.com/>`__ clusters
  managed with `Cloudera Manager
  <http://www.cloudera.com/content/cloudera/en/products-and-services/cloudera-enterprise/cloudera-manager.html>`__.

.. |hadoop| replace:: **Hadoop:**
.. _hadoop: installation/hadoop/index.html

- |hadoop|_ Installing CDAP on Hadoop systems, either **generic Apache Hadoop** distributions, 
  CDH clusters *not* managed with Cloudera Manager, or HDP clusters *not* managed with Apache Ambari.

.. |mapr| replace:: **MapR:**
.. _mapr: installation/mapr/index.html

- |mapr|_ Installing CDAP on `MapR systems <https://www.mapr.com>`__.

.. |sdk| replace:: **CDAP Standalone SDK:**
.. _sdk: ../developers-manual/getting-started/standalone/index.html

- |sdk|_ Installing the CDAP Standalone SDK on Linux, MacOS, and Windows systems. *(Developers' Manual)*


.. rubric:: Security

.. |security| replace:: **Security:**
.. _security: security.html

|security|_ CDAP supports **securing clusters using a perimeter security model.** This
section describes enabling security, configuring authentication, testing security, and includes an
example configuration file.


.. rubric:: Operations

.. |operations| replace:: **Operations:**
.. _operations: operations/index.html

|operations|_ Covers **logging, metrics, monitoring, preferences, scaling instances, resource guarantees, 
transaction service maintenance,** and **introduces the CDAP UI.** 

.. |logging| replace:: **Logging:**
.. _logging: operations/logging.html

- |logging|_ Covers **CDAP support for logging** through standard SLF4J (Simple Logging Facade for Java) APIs.

.. |metrics| replace:: **Metrics:**
.. _metrics: operations/metrics.html

- |metrics|_ CDAP collects **metrics about the application’s behavior and performance**.
  
.. |monitoring| replace:: **Monitoring:**
.. _monitoring: operations/monitoring.html

- |monitoring|_ CDAP collects **logs and metrics** for all of its internal services. 
  This section provides links to the relevant APIs for accessing these logs and metrics.

.. |preferences| replace:: **Preferences and Runtime Arguments:**
.. _preferences: operations/preferences.html

- |preferences|_ Flows, MapReduce programs, services, workflows, and workers can receive **runtime arguments.**

.. |scaling-instances| replace:: **Scaling Instances:**
.. _scaling-instances: operations/scaling-instances.html

- |scaling-instances|_ Covers **querying and setting the number of instances of flowlets and services.** 

.. |resource-guarantees| replace:: **Resource Guarantees:**
.. _resource-guarantees: operations/resource-guarantees.html

- |resource-guarantees|_ Providing resource guarantees **for CDAP programs in YARN.**

.. |tx-maintenance| replace:: **Transaction Service Maintenance:**
.. _tx-maintenance: operations/tx-maintenance.html

- |tx-maintenance|_ Periodic maintenance of the **Transaction Service.**

.. |cdap-ui| replace:: **CDAP UI:**
.. _cdap-ui: operations/cdap-ui.html

- |cdap-ui|_ The CDAP UI is available for **deploying, querying, and managing CDAP.** 


.. rubric:: Appendices

.. |appendices| replace:: **Appendices:**
.. _appendices: appendices/index.html

- |appendices| Two appendices cover the XML files used to configure the 
  :ref:`CDAP installation <appendix-cdap-site.xml>` and the :ref:`security configuration.
  <appendix-cdap-security.xml>`
