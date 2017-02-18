.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

.. _admin-index:

==========================
CDAP Administration Manual
==========================

Covers putting CDAP into production, with **components, system requirements, deployment
architectures, Hadoop compatibility, installation, configuration, security setup, and
operations.** Appendices describe the **XML files** used to configure the CDAP
installation and its security configuration.


.. |cdap-components| replace:: **CDAP Components**
.. _cdap-components: cdap-components.html

- |cdap-components|_


.. |deployment-architectures| replace:: **Deployment Architectures:**
.. _deployment-architectures: deployment-architectures.html

- |deployment-architectures|_ **Minimal** and **high availability, highly scalable** deployments.


.. |hadoop-compatibility| replace:: **Hadoop Compatibility:**
.. _hadoop-compatibility: hadoop-compatibility.html

- |hadoop-compatibility|_ The **Hadoop/HBase environment** that CDAP requires.


.. |system-requirements| replace:: **System Requirements:**
.. _system-requirements: system-requirements.html

- |system-requirements|_ Hardware, memory, core, and network **requirements**, software
  **prerequisites**, and using CDAP with **firewalls**.


.. |installation| replace:: **Installation:**
.. _installation: installation/index.html

- |installation|_ Installation and configuration instructions for either **specific
  distributions** using a distribution manager or **generic Apache Hadoop** clusters using
  RPM or Debian Package Managers:

    .. |cloudera| replace:: **Cloudera Manager:**
    .. _cloudera: installation/cloudera.html

    - |cloudera|_ Installing on `CDH (Cloudera Distribution of Apache Hadoop) <http://www.cloudera.com/>`__ 
      clusters managed with `Cloudera Manager
      <http://www.cloudera.com/content/cloudera/en/products-and-services/cloudera-enterprise/cloudera-manager.html>`__.

    .. |emr| replace:: **Amazon Hadoop (EMR):**
    .. _emr: installation/emr.html

    - |emr|_ Installing on `Amazon EMR (Elastic MapReduce) <https://aws.amazon.com/emr/>`__.

    .. |ambari| replace:: **Apache Ambari:**
    .. _ambari: installation/ambari.html

    - |ambari|_ Installing on `HDP (Hortonworks Data Platform)
      <http://hortonworks.com/>`__ clusters managed with `Apache Ambari
      <https://ambari.apache.org/>`__.

    .. |mapr| replace:: **MapR:**
    .. _mapr: installation/mapr.html

    - |mapr|_ Installing on the `MapR Converged Data Platform <https://www.mapr.com>`__.

    .. |packages| replace:: **Manual Installation using Packages**
    .. _packages: installation/packages.html

    - |packages|_ Installing on generic Apache Hadoop clusters, CDH (Cloudera
      Distribution of Apache Hadoop) clusters *not managed* with Cloudera Manager, or HDP
      (Hortonworks Data Platform) clusters *not managed* with Apache Ambari

    .. |replication| replace:: **Replication**
    .. _replication: installation/replication.html

    - |replication|_ Covers the replication of CDAP clusters from a master to one or more slave clusters


.. |verification| replace:: **Verification:**
.. _verification: verification.html

- |verification|_ How to verify the CDAP installation on your Hadoop cluster by using an
  **example application** and **health checks**.


.. |upgrading| replace:: **Upgrading:**
.. _upgrading: upgrading/index.html

- |upgrading|_ Instructions for upgrading both CDAP and its underlying Hadoop distribution.


.. |security| replace:: **Security:**
.. _security: security/index.html

- |security|_ CDAP supports securing clusters using a **perimeter security**,
  **authorization**, **impersonation**, **SSL for system services**, and **secure storage**.
  This section describes enabling, configuring, and testing security. It also provides
  example configuration files.


.. |operations| replace:: **Operations:**
.. _operations: operations/index.html

- |operations|_

    .. |logging| replace:: **Logging and Monitoring:**
    .. _logging: operations/logging.html

    - |logging|_ CDAP collects **logs** for all of its internal services and user
      applications; at the same time, CDAP can be **monitored through external systems**.
      Covers **log location**, **logging messages**, the **master services logback
      configuration** and **CDAP support for logging** through standard SLF4J (Simple
      Logging Facade for Java) APIs.

    .. |metrics| replace:: **Metrics:**
    .. _metrics: operations/metrics.html

    - |metrics|_ CDAP collects **metrics about the application’s behavior and performance**.
  
    .. |preferences| replace:: **Preferences and Runtime Arguments:**
    .. _preferences: operations/preferences.html

    - |preferences|_ Flows, MapReduce and Spark programs, services, workers, and workflows can receive **runtime arguments.**

    .. |scaling-instances| replace:: **Scaling Instances:**
    .. _scaling-instances: operations/scaling-instances.html

    - |scaling-instances|_ Covers **querying and setting the number of instances of flowlets, services, and workers.** 

    .. |resource-guarantees| replace:: **Resource Guarantees:**
    .. _resource-guarantees: operations/resource-guarantees.html

    - |resource-guarantees|_ Providing resource guarantees **for CDAP programs in YARN.**

    .. |tx-maintenance| replace:: **Transaction Service Maintenance:**
    .. _tx-maintenance: operations/tx-maintenance.html

    - |tx-maintenance|_ Periodic maintenance of the **Transaction Service.**

    .. |cdap-ui| replace:: **CDAP UI:**
    .. _cdap-ui: operations/cdap-ui.html

    - |cdap-ui|_ The CDAP UI is available for **deploying, querying, and managing CDAP.** 


.. |appendices| replace:: **Appendices:**
.. _appendices: appendices/index.html

- |appendices|_

  - :ref:`Appendix: Minimal cdap-site.xml <appendix-minimal-cdap-site.xml>`: Minimal required configuration for a CDAP installation
  - :ref:`Appendix: cdap-site.xml <appendix-cdap-site.xml>`: Default properties for a CDAP installation
  - :ref:`Appendix: cdap-security.xml <appendix-cdap-security.xml>`: Default security properties for a CDAP installation
  - :ref:`Appendix: HBaseDDLExecutor <appendix-hbase-ddl-executor>`: Example implementation and description for replication
