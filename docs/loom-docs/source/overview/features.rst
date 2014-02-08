.. _overview_features:
.. include:: ../toplevel-links.rst

.. _features:
========
Features
========


Loom provides a number of core set of complementary features to provision and monitor server clusters. Collectively, they
make large scale deployment manageable. 

Core Features
=============
• Simple on-demand cluster provisioning
• Automatic placement and management of services during cluster creation based on constraint based templates and desired cluster size
• Seamless integration with OpenStack and IaaS providers
• Pluggable automation platform (e.g. Chef, Puppet)
• Scalability to hundreds of clusters
• Modular configuration and service management
• UI for admin to create and manage configuration, and for user to customize and create clusters.
• Fully driven by REST APIs


Other Features
==============
• Pre-defined templates for clusters like Hadoop and LAMP
• Out of the box startup with in-memory zookeeper and embedded DB
• Uses Chef solo as SCM engine, hence not dependent on a Chef server
• Push model for provisioning and install, hence can provision clusters outside firewall
• One click import and export of Loom catalog and associated entities 
• Status updates during cluster creation in UI 
• User defineable configuration for cluster creation
• Centralized dashboard to view and manage multiple clusters.

Reduced IT Overhead
^^^^^^^^^^^^^^^^^^^
In many organizations, developers submit requests to access a Hadoop cluster
in order to run a MapReduce job. With Loom, the IT department sets up a
catalog of clusters that can be provisioned directly by developers. Developers
can allocate or destroy a cluster directly from their workstation.
Instant access to IT resources reduces wait time and increases productivity.
Loom provides a centralized dashboard to view and manage multiple clusters.

Private and Public Clouds
^^^^^^^^^^^^^^^^^^^^^^^^^
As well as working and integrating with any IaaS provider in the public cloud, Loom also integrates 
easily with OpenStack for behind-the-firewall cluster provisioning and management. 

Seamless Enterprise Integration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Loom simplifies the installation and configuration of any software stack,
including Hadoop, and it ensures that all installations are verified before they
are made available. 

Extensibility
^^^^^^^^^^^^^
Through the open source automation platform Chef, you can manage any Big Data application. To that end, 
many Chef recipes are readily available, and as an administrator or developer, you can develop your own.

REST APIs and a Rich UI
^^^^^^^^^^^^^^^^^^^^^^^
Integrate with existing tools and workflows via the Loom REST API. Loom also
provides an intuitive UI that allows users to create and manage clusters.
