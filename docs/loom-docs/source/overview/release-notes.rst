.. _overview_release-notes:

.. index::
   single: Release Notes
========
Release Notes
========
.. _release-notes:

Welcome to Loom 0.9.5 Beta Release. In today's release, we have updated Loom Server, Loom Provisioners, and Loom UI. Loom overall has new and improved functionality and ton of bug fixes.

We hope you enjoy this release.  If you encounter any issues, please don't hesitate to post on our `Continuuity support portal
<https://continuuity.uservoice.com/clients/widgets/classic_widget?mode=support&link_color=162e52&primary_color=42afcf&embed
_type=lightbox&trigger_method=custom_trigger&contact_enabled=true&feedback_enabled=false&smartvote=true&referrer=http%3A%2F%2Fcontinuuity.com%2F#contact_us>`_.

Fixed Issues
^^^^^^^^^^^^^
• Layout solver used to take minutes for solving large (> 400) node clusters
• Resistance to transient zookeeper connection loss
• Provisioner was made more resilient to transient issues.

New Features
^^^^^^^^^^^^^
• On-demand cluster provisioning 
• Support for any type of cluster with constraint based templates 
• Pre-defined templates for clusters like Hadoop and LAMP  
• Integration with OpenStack and IaaS providers
• Pluggable automation platform (e.g. Chef, Puppet)
• Scalability to hundreds of clusters
• Modular approach to configuration and service management 
• Out of the box startup with in-memory zookeeper and embedded DB 
• User defineable configuration for cluster creation
• Uses Chef solo as SCM engine, hence not dependent on a Chef server
• Push model for provisioning and installing, hence can provision clusters outside firewall
• UI for administrators to create and manage configuration, and for users to customize and create clusters. 
• Fully driven by REST APIs 

Released Versions
^^^^^^^^^^^^^
• |release|
• 0.5.0 Alpha
• 0.1.0  

Known Issues
^^^^^^^^^^^^^
• Potential for node data to grow beyond persistent store cell limit  
• Minimal authentication 
• Key files must be stored in plugin 
• Provisioner does not enforce timeouts 

Licenses
^^^^^^^^
This section specifies all the components and their respective :doc:`licenses <licenses>` that are used in Loom Server, Loom UI and Loom Provisioner

