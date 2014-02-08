:orphan:

.. _faq_toplevel:
.. include:: /toplevel-links.rst

====================================
General
====================================

What are the differences between Loom and Ambari/Savanna?
----------------------------------------------------------

.. figure:: loom-ambari-comparision.png
    :align: center
    :width: 800px
    :alt: Loom Ambari Comparision
    :figclass: align-center


.. list-table::
   :widths: 15 10 15 25
   :header-rows: 1

Following are some high level features that differentiate Loom from other system in this domain - mainly Apache Ambari and Savanna.

   * - Feature
     - Loom
     - Ambari/Savanna
     - Description
   * - Multi Cluster support
     - Y
     - N
     - Ambari/Savnna has limited support in backend, single cluster support on frontend in Ambari.
   * - Private or Public Cloud support
     - Y
     - N
     - No provisioning capability in Ambari, Savvana supports but both are limited to only hadoop clusters.
   * - Support for other OS
     - Y 
     - N
     - Only supports RHEL and SuSE. More information `AMBARI-1241 <https://issues.apache.org/jira/browse/AMBARI-1241>`_ and `AMBARI-660 <https://issues.apache.org/jira/browse/AMBARI-660>`_
   * - Support any distribution of Hadoop 
     - Y
     - N
     - Ambari only supports HDP, `AMBARI-3524 <https://issues.apache.org/jira/browse/AMBARI-3524>`_
   * - Support for any automation system
     - Y
     - N
     - Ambari only supports Puppet. Loom can support any automation framework through :doc:`Provisioner Plugins </guide/admin/plugins>`
   * - Cluster template support 
     - Y
     - Y/N
     - Ambari supports blueprint after the cluster is available for further management. But doesn't use them to create template. Savvana does, but it has to define node groups for placing services on each node group. Loom has built intelligence for solving the service placement through a layout planner.
   * - Consistency Guarantees
     - Y
     - N
     - Ambari doesn't guarantee consistency in case of failures during provisioning. Loom layout planner DAG executioner makes every attempt to keep the cluster in a sane state by transacting operations being done on the cluster.

Does Loom work with Ambari?
-------------------------------------------------
Loom is designed to support importing instance of a template as Apache Ambari
Blueprint once it's available in Ambari. Please refer to
`AMBARI-1783 <https://issues.apache.org/jira/browse/AMBARI-1783>`_ for more information.

What are the differences between Loom and Amazon EMR?
----------------------------------------------------------------
Amazon EMR provides a subset of Hadoop services (Hive, Pig, HBase, and MapReduce) and manages 
jobs and workflows on top of those services. Loom is a software agnostic, generic system for 
creating clusters of any layout and of any type. Being software agnostic, Loom has no support
for managing jobs on clusters, as its focus is on cluster creation and management. 

What providers  are supported by Loom ?
---------------------------------------
Out-of-box we support Rackspace, Joyent, Openstack (fog compatible). In near future plugins
for Azure, BlueBox, EC2, GCE & Terremark.

Does Loom make it easy for me to move from one cloud to another ?
----------------------------------------------------------------
Absolutely, when we built it at Continuuity, main goal was to make it seamless to migrate from 
one cloud to another.

Can Loom work on my laptop ?
-----------------------------
Loom is built using Java, NodeJS & Ruby and it's totally possible to run it on your laptop. We 
haven't yet tested and we believe that in future releases this will be available.

How long has Loom been used in a production enviroment and where is it being used?
----------------------------------------------------------------------------------
A previous version of Loom has been running in production since Feb 2012 at Continuuity.

Is Loom designed only for provisioning computers and storage?
-----------------------------------------------------------
It's a generic provisioning coordination system and can be used for provisioning more than 
just computers and storage. It's not been tested, but the architecture supports provisioning 
and configuring of other resources. Please refer to :doc:`Administration </guide/admin/index>` 
for more details on how to write plugins for provisioners to support different Provider
and Automator for provisioning and configuring different resources.

What is the recommended setup for Loom in terms of hardware and configuration?
------------------------------------------------------------------------------
We recommend the following :doc:`deployment </guide/recommended-deployment>` for production envionment that includes
HA for persistence store, multiple nodes for zookeeper, and HA proxy for traffic distribution across UI and provisioners.

Does Loom support monitoring and alerting of services deployed ?
----------------------------------------------------------------
Currently, it doesn't. But, another system within Continuuity named Mensa (A monitoring and alerting system) is being integrated
into Loom to support monitoring and alerting.

Does Loom support metering ?
----------------------------
Loom internally keeps track of cluster, resources and services being materialized through templates for each account. This information
will be exposed through administration interface in next release.
