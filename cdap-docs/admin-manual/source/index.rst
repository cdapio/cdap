.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _admin-index:

==================================================
CDAP Administration Manual
==================================================


.. rubric:: Installation

**Installation:** Covers **putting CDAP into production, with installation, configuration, security setup, and
monitoring.** Appendices cover the XML files used to configure the CDAP installation and security configurations.

.. |installation| replace:: **Installation and Configuration:**
.. _installation: installation/installation.html

.. |security| replace:: **Security:**
.. _security: installation/security.html

.. |monitoring| replace:: **Monitoring:**
.. _monitoring: installation/monitoring.html

.. |appendices| replace:: **Appendices:**

- |installation|_ Covers **installing and configuring CDAP:** the system, network, and software
  requirements; packaging options; and the instructions for installation and verification of the
  CDAP components so they work with your existing Hadoop cluster.

- |security|_ CDAP supports securing clusters using a perimeter security model. This section
  described enabling security, configuring authentication, testing security, and includes 
  an example configuration file.

- |monitoring|_ CDAP collects logs and metrics for all of its internal services. 
  This section provides links to the relevant APIs for accessing these logs and metrics.

- |appendices| Two appendices cover the XML files used to configure the 
  :ref:`CDAP installation <appendix-cdap-site.xml>` and the :ref:`security configuration.
  <appendix-cdap-security.xml>`


.. rubric:: Operations

**Operations:** Covers **logging, metrics, runtime arguments, scaling instances and 
introduces the CDAP Console.** 

.. |logging| replace:: **Logging:**
.. _logging: operations/logging.html

.. |metrics| replace:: **Metrics:**
.. _metrics: operations/metrics.html

.. |runtime-arguments| replace:: **Runtime Arguments:**
.. _runtime-arguments: operations/runtime-arguments.html

.. |scaling-instances| replace:: **Scaling Instances:**
.. _scaling-instances: operations/scaling-instances.html

.. |cdap-console| replace:: **CDAP Console:**
.. _cdap-console: operations/cdap-console.html

- |logging|_ Covers **CDAP support for logging** through standard SLF4J (Simple Logging Facade for Java) APIs.

- |metrics|_ CDAP collects **metrics about the application’s behavior and performance**.
  
- |runtime-arguments|_ Flows, Procedures, MapReduce Jobs, and Workflows can receive **runtime arguments:** 

- |scaling-instances|_ Covers **querying and setting the number of instances of Flowlets and Procedures.** 

- |cdap-console|_ The CDAP Console is available for **deploying, querying and managing CDAP.** 



.. |(TM)| unicode:: U+2122 .. trademark sign
   :ltrim:

.. |(R)| unicode:: U+00AE .. registered trademark sign
   :ltrim:
