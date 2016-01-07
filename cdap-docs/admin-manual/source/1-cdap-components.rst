.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _admin-manual-cdap-components:

===============
CDAP Components
===============

CDAP Components
===============

These are the CDAP components:

- **CDAP Master:** Service for managing runtime, lifecycle and resources of CDAP applications (package *cdap-master*);
- **CDAP Router:** Service supporting REST endpoints for CDAP (package *cdap-gateway*);
- **CDAP UI:** User interface for managing CDAP applications (package *cdap-ui*);
- **CDAP Kafka:** Metrics and logging transport service, using an embedded version of *Kafka* (package *cdap-kafka*); and
- **CDAP Authentication Server:** Performs client authentication for CDAP when security is enabled (package *cdap-security*).

Some CDAP components run on YARN, while others orchestrate “containers” in the Hadoop cluster.
The CDAP Router service starts a router instance on each of the local boxes and instantiates
one or more gateway instances on YARN as determined by the gateway service configuration.

In addition to these components, CDAP uses configuration XML files. The basic
configuration is set in the ``cdap-site.xml`` file (documented with in :ref:`an appendix
<appendix-cdap-site.xml>`). This file is automatically created when you follow the
installation procedure steps detailed in later sections. You edit a version of this file
to configure CDAP to your specific requirements prior to starting CDAP services.

If you have :ref:`CDAP Security <admin-security>` enabled, then you will have an
additional file, ``cdap-security.xml`` (documented in :ref:`an appendix
<appendix-cdap-security.xml>`), with additional settings.

Installation Summary
====================

In summary, these are the steps you follow to install CDAP.

If you are installing on a generic Apache Hadoop cluster:

#. Determine your system architecture
#. Review and meet the system requirements: hardware, network, and software
#. Prepare your Hadoop cluster 
#. Install the CDAP components
#. Configure the CDAP installation
#. Start CDAP services
#. Verify the CDAP installation by running an example application


If, on the other hand, you are using a supported distribution and manager (CDH and
Cloudera Manager, HDP and Apache Ambari, or MapR), the steps are:

#. Determine your system architecture
#. Review and meet the system requirements: hardware, network, and software
#. Prepare your Hadoop cluster 
#. Install the CDAP components
#. Configure the CDAP installation
#. Start CDAP services
#. Verify the CDAP installation by running an example application