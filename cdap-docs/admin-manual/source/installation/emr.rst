.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. :section-numbering: true

.. _admin-installation-emr:

==================================================
Installation on Amazon EMR using Bootstrap Actions
==================================================

Introduction
============

This section describes installing CDAP on Amazon EMR clusters using
the `Amazon EMR "Run If" Bootstrap Action 
<http://docs.aws.amazon.com/ElasticMapReduce/latest/ManagementGuide/emr-plan-bootstrap.html#emr-bootstrap-runif>`__
to:

- Install necessary EMR components;
- Restrict CDAP installation to the EMR master node;
- Download, install, and automatically configure CDAP for EMR; and
- Run all services as the ``'cdap'`` user

Information on Amazon EMR is `available online
<http://docs.aws.amazon.com/ElasticMapReduce/latest/ManagementGuide/emr-what-is-emr.html>`__.

CDAP |short-version| is compatible with Amazon EMR 4.6.0 through 4.8.2.


Using the Create Cluster Wizard
===============================

**Note:** For any settings not listed or specified below, we recommend using the default settings.

1. Open the Amazon EMR console at https://console.aws.amazon.com/elasticmapreduce/.

#. Choose "Create cluster."

#. In the *Advanced Options*, *Step 1: Software and Steps*, set:

   - Vendor: Amazon
   - Release: ``emr-4.6.0`` through ``emr-4.8.2`` 
   - Software: Hadoop, HBase, Hive, Spark
   - No auto-terminate

   .. figure:: ../_images/emr/emr-step1-software-and-steps.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **EMR Create Cluster Wizard:** Step 1: Software and Steps

#. In *Step 2: Hardware*, set:

   - Network: use defaults
   - EC2 Subnet: use defaults
   - Master
   
     - EC2 Instance type: ``m3.xlarge``
     - Instance count: 1
     
   - Core
   
     - EC2 Instance type: ``m3.xlarge``
     - Instance count: 4 (as a minimum)
     
   - Task
   
     - Instance count: 0 (not required)

   .. figure:: ../_images/emr/emr-step2-hardware.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **EMR Create Cluster Wizard:** Step 2: Hardware

#. In *Step 3: General Cluster Settings*, set:

   - Logging
   - Debugging
   - Termination protection (no auto-terminate)

   .. figure:: ../_images/emr/emr-step3-general-cluster-settings.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **EMR Create Cluster Wizard:** Step 3: General Cluster Settings

#. In *Step 3: General Cluster Settings*, add a *Bootstrap Action:*

   - Type: *Run If*
   - Optional arguments:
   
     .. parsed-literal::
   
       instance.isMaster=true "curl \http://downloads.cask.co/emr/install-|release|.sh | sudo bash -s"

   .. figure:: ../_images/emr/emr-step3b-bootstrap-action-run-if.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **EMR Create Cluster Wizard:** Add Bootstrap Action

#. In *Step 4: Security*, set following defaults, and then add a security group (next step).

   .. figure:: ../_images/emr/emr-step4-security.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **EMR Create Cluster Wizard:** Step 4: Security

#. In *Step 4: Security*, set additional *EC2 Security Groups* to the master node:

   - Master (one of the following):

     - A Security Group with ports 11011/11015 open; *or*
     - An `SSH Tunnel <https://docs.aws.amazon.com/ElasticMapReduce/latest/ManagementGuide/emr-web-interfaces.html>`__

   .. figure:: ../_images/emr/emr-step4b-additional-security-group.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **EMR Create Cluster Wizard:** Assigning additional security group to master node

Once the cluster is created, CDAP services will start up. This will take about 10 minutes
after the cluster is in a *Waiting* state.

Verification
============

.. include:: /_includes/installation/smoke-test-cdap.txt
