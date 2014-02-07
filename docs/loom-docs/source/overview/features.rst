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
• On-demand Hadoop cluster provisioning
• Scalability to hundreds of clusters
• Atomic cluster operations
• Seamless integration with OpenStack and IaaS providers
• Flexible storage and compute models (VM, KVM, LXC)
• Pluggable automation platform (e.g. Chef, Puppet)
• Cluster management across datacenters
• Monitoring and integration with Ganglia

Other Features
==============

Full Lifecycle Management
^^^^^^^^^^^^^^^^^^^^^^^^^
Loom allows developers to scale-up or scale-down clusters using the
Loom UI and command line tools. Administrators and users can upgrade, downgrade, or
install new software to any existing cluster.

Reduced IT Overhead
^^^^^^^^^^^^^^^^^^^
In many organizations, developers submit requests to access a Hadoop cluster
in order to run a MapReduce job. With Loom, the IT department sets up a
catalog of clusters that can be provisioned directly by developers. Developers
can allocate, scale or destroy a cluster directly from their workstation.
Instant access to IT resources reduces wait time and increases productivity.

Monitoring and Alerting
^^^^^^^^^^^^^^^^^^^^^^^
Loom provides a centralized dashboard to monitor and manage multiple clusters
concurrently. IT operations can centralize logs, custom metrics, and alerts for
instant remediation of critical issues. Integration with Ganglia and Hadoop
metrics is seamless.

Multiple Datacenters
^^^^^^^^^^^^^^^^^^^^
Loom provides a unified view of all data center capacity. Based on developer preference and
resource availability, it allows provisioning of clusters across multiple data centers.

Private and Public Clouds
^^^^^^^^^^^^^^^^^^^^^^^^^
As well as working and integrating with any IaaS provider in the public cloud, Loom also integrates 
easily with OpenStack for behind-the-firewall cluster provisioning and management. 

Seamless Enterprise Integration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Loom simplifies the installation and configuration of any software stack,
including Hadoop, and it ensures that all installations are verified before they
are made available. It also integrates seamlessly with existing LDAP
installations to enforce user permissions and with existing IT operations
databases to manage network resources.

Extensibility
^^^^^^^^^^^^^
Through the open source automation platform Chef, you can manage any Big Data application. To that end, 
many Chef recipes are readily available, and as an administrator or developer, you can develop your own.

REST APIs and a Rich UI
^^^^^^^^^^^^^^^^^^^^^^^
Integrate with existing tools and workflows via the Loom REST API. Loom also
provides an intuitive UI that allows users to create and manage clusters.
