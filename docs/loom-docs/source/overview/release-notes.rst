.. _overview_release-notes:

.. include:: ../toplevel-links.rst

========
Release Notes
========
.. _release-notes:

Welcome to Loom 1.0.0. In today's scheduled release, we have updated Loom Server, Loom Provisioners and Loom UI. Loom overall has new and improved functionality and bug fixes.

We hope you enjoy this release.  If you encounter any issues please don't hesitate to post on our `Continuuity support portal
<https://continuuity.uservoice.com/clients/widgets/classic_widget?mode=support&link_color=162e52&primary_color=42afcf&embed
_type=lightbox&trigger_method=custom_trigger&contact_enabled=true&feedback_enabled=false&smartvote=true&referrer=http%3A%2F%2Fcontinuuity.com%2F#contact_us>`_.

Fixed Issues
^^^^^^^^^^^^^
• Cluster layout solver rule fixes  
• Resistance to transient zookeeper connection loss

New Features
^^^^^^^^^^^^^
• On-demand cluster provisioning 
• Support for any type of cluster with contraint based templates 
• Pre-defined templates for clusters like Hadoop and LAMP  
• Integration with OpenStack and IaaS providers
• Pluggable automation platform (e.g. Chef, Puppet)
• Scalability to hundreds of clusters
• Modular approach to configuration and service management 
• Out of the box startup with in-memory zookeeper and embedded DB 
• User defineable configuration for cluster creation
• Uses Chef solo as SCM engine, hence not dependent on a Chef server
• Push model for provisioning and install, hence can provision clusters outside firewall
• UI for admin to create and manage configuration, and for user to customize and create clusters. 
• Fully driven by REST APIs 

Released Versions
^^^^^^^^^^^^^
• 1.0.0 
• 0.1.0  

Known Issues
^^^^^^^^^^^^^
• Poor solver performance for certain types of templates  
• Cluster creation cannot be aborted before the first task has started  
• Potential for node data to grow beyond persistent store cell limit  
• Minimal authentication 
• Key files must be stored in plugin 
• Provisioner does not enforce timeouts 

