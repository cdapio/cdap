.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

:hide-toc: true

.. _operations-index:

============================================
Operations
============================================

.. toctree::
   :maxdepth: 1
   
    Logging <logging>
    Metrics <metrics>
    Runtime Arguments <runtime-arguments>
    Scaling Instances <scaling-instances>
    CDAP Console <cdap-console>
    Troubleshooting <troubleshooting>
    

.. |logging| replace:: **Logging:**
.. _logging: logging.html

- |logging|_ Covers **CDAP support for logging** through standard SLF4J (Simple Logging Facade for Java) APIs.


.. |metrics| replace:: **Metrics:**
.. _metrics: metrics.html

- |metrics|_ CDAP collects **metrics about the application’s behavior and performance**.
  

.. |runtime-arguments| replace:: **Runtime Arguments:**
.. _runtime-arguments: runtime-arguments.html

- |runtime-arguments|_ Flows, Procedures, MapReduce programs, and Workflows can receive **runtime arguments:** 


.. |scaling-instances| replace:: **Scaling Instances:**
.. _scaling-instances: scaling-instances.html

- |scaling-instances|_ Covers **querying and setting the number of instances of Flowlets and Procedures.** 


.. |cdap-console| replace:: **CDAP Console:**
.. _cdap-console: cdap-console.html

- |cdap-console|_ The CDAP Console is available for **deploying, querying and managing CDAP.** 


.. |troubleshooting| replace:: **Troubleshooting:**
.. _troubleshooting: troubleshooting.html

- |troubleshooting|_ Selected examples of potential **problems and possible resolutions.**


.. rubric:: Command Line Interface

Most of the administrative operations are also available more conveniently through the
Command Line Interface. See :ref:`reference:cli` in the 
:ref:`CDAP Reference Manual<reference:reference-index>` for details.


.. rubric:: Getting a Health Check

.. _operations-health-check:

.. highlight:: console

Administrators can check the health of various services in the system.
(In these examples, substitute for ``<host>`` the host name or IP address of the CDAP server.)

- To retrieve the **health check of the CDAP Console**, make a GET request to the URI::

    http://<host>:9999/status

- To retrieve the **health check of the CDAP Router**, make a GET request to the URI::

    http://<host>:10000/status

- To retrieve the **health check of the CDAP Authentication Server**, make a GET request to
  the URI::
  
    http://<host>:10009/status

On success, the calls return a valid HTTP response with a 200 code.

- To retrieve the **health check of all the services running in YARN**, make a GET request
  to the URI::
  
    http://<host>:10000/v2/system/services

  On success, the call returns a JSON string with component names and their corresponding statuses.
