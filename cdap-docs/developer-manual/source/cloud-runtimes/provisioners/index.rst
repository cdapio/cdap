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
    Amazon Elastic MapReduce <aws-emr>
    Remote Hadoop <remote-hadoop>

Provisioners are responsible for creating, initializing, and destroying the cloud environment that pipelines
will run in. Each provisioner exposes a set of configuration settings that are used to control what type
of cluster is created and deleted. Different provisioners create different types of clusters.

.. |gcp-dataproc| replace:: **Google Dataproc:**
.. _gcp-dataproc: gcp-dataproc.html

.. |aws-emr| replace:: **Amazon Elastic MapReduce:**
.. _aws-emr: aws-emr.html

.. |remote-hadoop| replace:: **Remote Hadoop:**
.. _remote-hadoop: remote-hadoop.html

- |gcp-dataproc|_ A fast, easy-to-use, and fully-managed cloud service for running Apache Spark and Apache Hadoop clusters in a simple,
  cost efficient way.

- |aws-emr|_ Provides a managed Hadoop framework that makes it easy, fast, and cost effective to process vast amounts of data across
  dynamically scalable Amazon EC2 instances.

- |remote-hadoop|_ Runs jobs on a pre-existing Hadoop cluster, whether on premise or in the cloud.

