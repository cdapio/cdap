.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

:hide-toc: true

.. _cloudera-index:

==================================================
Cloudera
==================================================

.. toctree::
   :maxdepth: 1
   
    Configuring <configuring>
    Add Service Wizard, Step-by-Step <step-by-step-cloudera>
    Ingesting <ingesting>
    FAQ <faq>


.. |cloudera-configuring| replace:: **Configuring and Installing:**
.. _cloudera-configuring: configuring.html

- |cloudera-configuring|_ CDAP using Cloudera Manager


.. |cloudera-add-service| replace:: **Add Service Wizard:**
.. _cloudera-add-service: step-by-step-cloudera.html

- |cloudera-add-service|_ A **step-by-step guide to installing CDAP** using the Cloudera Manager *Add Service* Wizard


.. |cloudera-ingesting| replace:: **Ingesting:**
.. _cloudera-ingesting: ingesting.html

- |cloudera-ingesting|_ **Ingesting and Exploration** of data using **Impala**


.. |cloudera-faq| replace:: **FAQ:**
.. _cloudera-faq: faq.html

- |cloudera-faq|_ for Cloudera and Impala


.. rubric:: Introduction to CDAP, Cloudera and Impala

The Cask™ Data Application Platform (CDAP) integrates with the `Cloudera Manager
<http://www.cloudera.com/content/cloudera/en/products-and-services/cloudera-enterprise/cloudera-manager.html>`__.
Configurations that include Cloudera Manager can be easily configured to ingest data into
a cluster, specify schema, or run interactive queries using Impala with CDAP for faster
results. 


.. _integrations-impala:

.. rubric:: Integrating CDAP with Impala

When using CDAP with Impala:

.. |adapters| replace:: **CDAP Adapters:**
.. _adapters: ../../developers-manual/advanced/adapters.html

- |adapters|_ **Stream-conversion** and **custom adapter** types


.. |stream| replace:: **Stream Exploration:**
.. _stream: ../../developers-manual/data-exploration/streams.html

- |stream|_ It is often useful to be able to **examine data in a Stream** in an ad-hoc manner through SQL-like queries


.. |fileset| replace:: **Fileset Exploration:**
.. _fileset: ../../developers-manual/data-exploration/filesets.html

- |fileset|_ The **CDAP FileSet Datasets** can be explored through ad-hoc SQL-like queries.
