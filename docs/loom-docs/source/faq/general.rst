:orphan:

.. _faq_toplevel:

.. index::
   single: FAQ: General
====================================
General
====================================

What are the differences between Loom and Ambari/Savanna?
---------------------------------------------------------

From the `Apache Ambari <http://ambari.apache.org/>`_ site:

.. epigraph:: "The Apache Ambari project is aimed at making Hadoop management simpler by developing software for provisioning, managing, and monitoring Apache Hadoop clusters. Ambari provides an intuitive, easy-to-use Hadoop management web UI backed by its RESTful APIs."

From the `Savanna <https://savanna.readthedocs.org/en/latest/>`_ site:

.. epigraph:: "Savanna project aims to provide users with simple means to provision a Hadoop cluster at OpenStack by specifying several parameters like Hadoop version, cluster topology, nodes hardware details..."

Even though Loom may seem similar to Apache Ambari and Savanna in some aspects, it is vastly different in terms of functionality and system capabilities.

.. figure:: loom-ambari-comparision.png
    :align: center
    :width: 800px
    :alt: Loom Ambari Comparision
    :figclass: align-center


The graph above depicts Loom's management capabilities on the horizontal scale, and the table below differentiates the high level features absent in Apache Ambari and Savanna:

.. list-table::
   :widths: 15 10 15 25
   :header-rows: 1

   * - Feature
     - Loom
     - Ambari/Savanna
     - Description
   * - Multi Cluster support
     - Y
     - N
     - Ambari/Savanna offers limited support in the backend, and Ambari limits to single cluster in the frontend.
   * - Private or Public Cloud support
     - Y
     - N
     - No provisioning capability in Ambari. Savanna supports provisioning, but both are limited only to Hadoop clusters.
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
     - Ambari supports blueprints after the cluster is available for further management. Unlike Loom, it does not use templates to create clusters. Savanna, on the other hand, does support, but it requires node groups to be explicitly defined for placing services on each node group. Loom offers a built-in logic for solving the service placement through a layout planner.
   * - Consistency Guarantees
     - Y
     - N
     - Ambari does not guarantee consistency in case of failures during provisioning. In contrast, Loom layout planner DAG executioner ensures consistency by transacting operations on the cluster.

Does Loom work with Ambari?
---------------------------
Loom can export templates that are compatible with Apache Ambari blueprints. When this compatibility feature is
completed in Ambari, you may be able to work with these templates. Please refer to `AMBARI-1783 <https://issues.apache.org/jira/browse/AMBARI-1783>`_ for more information.

What are the differences between Loom and Amazon EMR?
-----------------------------------------------------
Amazon EMR provides a subset of Hadoop services (such as Hive, Pig, HBase, and MapReduce) and manages 
jobs and workflows on top of those services. Loom, on the other hand, is a software agnostic, generic system for 
creating clusters of any layout and of any type. Being software agnostic and a provisioning tool, Loom has no direct support
for managing jobs on clusters, as its focus is on cluster creation and management, not jobs and workflow management.

Will Loom support `docker <http://docker.io>`_ based clusters?
--------------------------------------------------------------
We believe in the potential of docker based clusters. In the future releases, we plan to support Docker based clusters.

Does Loom support bare metal?
-----------------------------
Yes.

What providers are supported by Loom?
-------------------------------------
Out of the box, Loom supports Rackspace, Joyent, Openstack (fog compatible). In the near future, plugins
for HP Cloud, Cloudstack, Azure, BlueBox, EC2, GCE, CloudFoundry, and Terremark will be supported.

Does Loom make it easy for me to migrate from one cloud to another?
-------------------------------------------------------------------
Absolutely. When we originally built Loom at Continuuity, the main goal was to make it a seamless process to migrate from
one cloud to another.

Can Loom work on my laptop?
---------------------------
Loom has yet to be tested on a laptop; however, it uses Java, NodeJS, and Ruby, so it may very well work.
Support for this functionally will be included in future releases.

How long has Loom been used in a production environment and where is it being used?
-----------------------------------------------------------------------------------
A previous version of Loom has been running in production at Continuuity since Feb 2012.

Is Loom designed only for provisioning compute and storage?
-------------------------------------------------------------
Loom is a generic provisioning coordination system, and it can be used for provisioning more than
just compute and storage. Though Loom has not yet been tested, the architecture supports provisioning
and configuring of other resources. Please refer to the :doc:`Provisioner Plugins</guide/admin/plugins>` page
for more details on how to write plugins for provisioners to support Providers and Automators that can provision and 
configure different resources.

What is the recommended setup for Loom in terms of hardware and configuration?
------------------------------------------------------------------------------
We recommend the following :doc:`deployment configuration </guide/recommended-deployment>` for a production environment that includes
HA for persistence store, multiple nodes for Zookeeper, and HA proxy for traffic distribution across UIs and provisioners.

Does Loom support monitoring and alerting of services deployed?
----------------------------------------------------------------
Currently, it does not; however, another system within Continuuity named Mensa (A monitoring and alerting system) is being integrated
into Loom to support monitoring and alerting.

Does Loom support metering?
---------------------------
For each account, and the templates from which it provisions resources, Loom internally keeps track of clusters, resources, and services. This information
will be exposed through the administration interface in the next release.

I use puppet. Will I be able to use puppet with Loom?
-----------------------------------------------------
Yes. Loom is a smart orchestration layer with open support for integrating any automation framework. You can use your puppet modules
to configure clusters. Please refer to the :doc:`Administration Guide </guide/admin/index>` for more details on how to integrate.

Can Loom support approval workflows or the ability to pause provisioning for approval?
--------------------------------------------------------------------------------------
The current version of Loom does not support it, but it will be very easy to add a cluster provisioning state for approval or pausing.

