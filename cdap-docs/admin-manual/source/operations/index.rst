.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

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


.. |logging| replace:: **Logging:**
.. _logging: logging.html

- |logging|_ Covers **CDAP support for logging** through standard SLF4J (Simple Logging Facade for Java) APIs.


.. |metrics| replace:: **Metrics:**
.. _metrics: metrics.html

- |metrics|_ CDAP collects **metrics about the application’s behavior and performance**.
  

.. |runtime-arguments| replace:: **Runtime Arguments:**
.. _runtime-arguments: runtime-arguments.html

- |runtime-arguments|_ Flows, Procedures, MapReduce Jobs, and Workflows can receive **runtime arguments:** 


.. |scaling-instances| replace:: **Scaling Instances:**
.. _scaling-instances: scaling-instances.html

- |scaling-instances|_ Covers **querying and setting the number of instances of Flowlets and Procedures.** 


.. |cdap-console| replace:: **CDAP Console:**
.. _cdap-console: cdap-console.html

- |cdap-console|_ The CDAP Console is available for **deploying, querying and managing CDAP.** 


.. rubric:: Command-Line Interface

Most of the administrative operations are also available more conveniently through the
Command-Line Interface. See :ref:`reference:cli` in the 
:ref:`CDAP Reference Manual<reference:reference-index>` for details.
