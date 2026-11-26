.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2018 Cask Data, Inc.

:hide-toc: true

.. _cloud-runtimes-provisioners:

============
Provisioners
============

.. toctree::
    :maxdepth: 4

    Google Dataproc <gcp-dataproc>

Provisioners are responsible for creating, initializing, and destroying the cloud environment that pipelines
will run in. Each provisioner exposes a set of configuration settings that are used to control what type
of cluster is created and deleted. Different provisioners create different types of clusters.

.. |gcp-dataproc| replace:: **Google Dataproc:**
.. _gcp-dataproc: gcp-dataproc.html

- |gcp-dataproc|_ A fast, easy-to-use, and fully-managed cloud service for running Apache Spark and Apache Hadoop clusters in a simple,
  cost efficient way.

