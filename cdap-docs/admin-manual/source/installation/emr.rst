.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2016 Cask Data, Inc.

:section-numbering: true

.. _admin-installation-emr:

==================================================
Installation on Amazon EMR using Bootstrap Actions
==================================================

This section describes installing CDAP on Amazon EMR clusters using
the ``"Run if"`` Bootstrap Action to:

- Install necessary EMR components
- Restrict CDAP installation to the EMR master node
- Download, install, and automatically configure CDAP for EMR
- Run all services as the ``'cdap'`` user

Using the Create Cluster wizard
===============================

Anything not listed, keep default

- Advanced
- Software and Steps
  - Vendor: Amazon
  - Release: 4.x
  - Hadoop, HBase, Hive, Spark
  - No auto-terminate
- Hardware
  - Network
  - Subnet
  - Master
  - Core
    - m3.xlarge x 4 (minimum)
  - Task
- General Cluster Settings
  - Bootstrap Actions
    - Run if
      - instance.isMaster=true "curl http://downloads.cask.co/emr/install-4.0.0.sh | sudo bash -s"
- Security
  - EC2 Security Groups
    - Master
      - Security Group with 11011/11015 open
      - or -
      - `SSH Tunnel <https://docs.aws.amazon.com/ElasticMapReduce/latest/ManagementGuide/emr-web-interfaces.html>`

Once the cluster is created, CDAP services will start up. This will take about 10 minutes after the cluster is
in a Waiting state.

Verification
============

.. include:: /_includes/installation/smoke-test-cdap.txt
