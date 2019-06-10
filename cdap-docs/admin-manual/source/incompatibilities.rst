.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2019 Cask Data, Inc.

.. _admin-manual-incompatibilities:

=============================
Incompatibility with CDAP 5.x
=============================

CDAP 6.0.0 has several incompatible changes. Due to this, CDAP 6.0.0 does not support upgrade or migration from
CDAP 5.x to 6.0.0. Below is the list of incompatibilities:

- **CDAP Applications and Custom Plugins:**

   * In CDAP 6.0.0, cdap packages have been renamed from `co.cask.cdap` to `io.cdap.cdap`. Before you can use CDAP
     applications and custom plugins in CDAP 6.0.0, you need to update and recompile them.
   * In CDAP 6.0.0, support for CDAP Flows and Streams have been removed. As an alternate, CDAP applications can rely
     on Spark Streaming.

- **CDAP Pipelines:**

   * All pipelines from existing CDAP clusters must be exported and re-imported into CDAP 6.0.0 clusters.

- **CDAP Operational Metadata:**

   * All the operational metadata such as logs, metrics, lineage collected by CDAP 5.x cluster,
     will not be migrated to CDAP 6.0.0.

- **CDAP Datasets:**

   * Dataset migration from CDAP 5.x to CDAP 6.0.0 is not supported.

- **Reusing Existing Hadoop Cluster:**

   * Upgrading an existing CDAP cluster is not supported in CDAP 6.0.0.

   * We recommend you install a CDAP 6.0.0 cluster on a new Hadoop cluster.
     However, if you decide to use an existing Hadoop cluster, avoid possible conflicts by using a different root
     namespace for the CDAP 6.0.0 installation. To change the root namespace, use the cdap-site property root.namespace.
     For the value of this property, use any name except for cdap.

Please reach out to cdap-dev@googlegroups.com for discussions about any issues related to CDAP 6.0.0 installation.