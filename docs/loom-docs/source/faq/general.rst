:orphan:

.. _faq_toplevel:
.. include:: /toplevel-links.rst
.. index::
   single: FAQ General
====================================
General
====================================

What are the differences between Loom and Ambari/Savanna?
---------------------------------------------------------

From the `Apache Ambari <http://ambari.apache.org/>`_ site:

.. epigraph:: "The Apache Ambari project is aimed at making Hadoop management simpler by developing software for provisioning, managing, and monitoring Apache Hadoop clusters. Ambari provides an intuitive, easy-to-use Hadoop management web UI backed by its RESTful APIs."

From the `Savanna <https://savanna.readthedocs.org/en/latest/>`_ site:

.. epigraph:: "Savanna project aims to provide users with simple means to provision a Hadoop cluster at OpenStack by specifying several parameters like Hadoop version, cluster topology, nodes hardware details..."

Even though Loom may seem similar to Apache Ambari and Savanna it is vastly different in terms of functionality and system capabilities.

.. figure:: loom-ambari-comparision.png
    :align: center
    :width: 800px
    :alt: Loom Ambari Comparision
    :figclass: align-center


Below are some high level features that differentiate Loom from Apache Ambari and Savanna:

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
     - Ambari/Savanna has limited support in backend, single cluster support on frontend in Ambari.
   * - Private or Public Cloud support
     - Y
     - N
     - No provisioning capability in Ambari. Savanna supports provisioning, but both are limited to only Hadoop clusters.
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
     - Ambari supports blueprints after the cluster is available for further management but does not use them to create templates. Savanna does, but requires node groups to be explicitly defined for placing services on each node group. Loom has built-in logic for solving the service placement through a layout planner.
   * - Consistency Guarantees
     - Y
     - N
     - Ambari does not guarantee consistency in case of failures during provisioning. Loom layout planner DAG executioner makes every attempt to keep the cluster in a sane state by transacting operations being done on the cluster.

Does Loom work with Ambari?
---------------------------
Loom is designed to support exporting of templates to be compatible with Apache Ambari blueprints when the feature is
completed in Ambari. Please refer to `AMBARI-1783 <https://issues.apache.org/jira/browse/AMBARI-1783>`_ for more information.

What are the differences between Loom and Amazon EMR?
-----------------------------------------------------
Amazon EMR provides a subset of Hadoop services (Hive, Pig, HBase, and MapReduce) and manages 
jobs and workflows on top of those services. Loom is a software agnostic, generic system for 
creating clusters of any layout and of any type. Being software agnostic, Loom has no direct support
for managing jobs on clusters, as its focus is on cluster creation and management. 

Will Loom support `docker <http://docker.io>`_ based clusters?
--------------------------------------------------------------
Loom will in the near future provide docker integration, as we believe in the potential of docker.

Does Loom support bare metal?
-----------------------------
Yes.

What providers are supported by Loom?
-------------------------------------
Out of the box, Loom supports Rackspace, Joyent, Openstack (fog compatible). In the near future, plugins
for HP Cloud, Cloudstack, Azure, BlueBox, EC2, GCE, CloudFoundry and Terremark will be supported.

Does Loom make it easy for me to move from one cloud to another?
----------------------------------------------------------------
Absolutely. When we originally built Loom at Continuuity, the main goal was to make it a seamless process to migrate from
one cloud to another.

Can Loom work on my laptop?
---------------------------
Loom has yet to be tested on a laptop, however, it uses Java, NodeJS and Ruby and may very well work.
Support for this functionally will be included in future releases.

How long has Loom been used in a production environment and where is it being used?
-----------------------------------------------------------------------------------
A previous version of Loom has been running in production at Continuuity since Feb 2012.

Is Loom designed only for provisioning compute and storage?
-----------------------------------------------------------
Loom is a generic provisioning coordination system and can be used for provisioning more than
just compute and storage. It has not yet been tested, but the architecture supports provisioning
and configuring of other resources. Please refer to the :doc:`Provisioner Plugins</guide/admin/plugins>` page
for more details on how to write plugins for provisioners to support Providers
and Automators that can provision and configure different resources.

What is the recommended setup for Loom in terms of hardware and configuration?
------------------------------------------------------------------------------
We recommend the following :doc:`deployment configuration </guide/recommended-deployment>` for a production environment that includes
HA for persistence store, multiple nodes for Zookeeper, and HA proxy for traffic distribution across UIs and provisioners.

Does Loom support monitoring and alerting of services deployed?
----------------------------------------------------------------
Currently, it does not, however, another system within Continuuity named Mensa (A monitoring and alerting system) is being integrated
into Loom to support monitoring and alerting.

Does Loom support metering?
---------------------------
Loom internally keeps track of clusters, resources and services being materialized through templates for each account. This information
will be exposed through the administration interface in the next release.

I use puppet. Will I be able to use puppet with Loom?
-----------------------------------------------------
Yes. Loom is a smart orchestration layer with open support for integrating any automation framework. You can use your puppet modules
to configure clusters. Please refer to the :doc:`Administration Guide </guide/admin/index>` for more details on how it can be integrated.

Can Loom support approval workflows or the ability to pause provisioning for approval?
--------------------------------------------------------------------------------------
The current version of Loom does not support it, but it will very easy to add a cluster provisioning state for approval or pausing.

