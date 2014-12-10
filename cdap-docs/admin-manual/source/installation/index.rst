.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

:hide-toc: true

.. _installation-index:

============================================
Installation
============================================

.. toctree::
   :maxdepth: 1
   
    Quick Start <quick-start>
    Installation and Configuration <installation>
    Security <security>
    Monitoring <monitoring>
    Appendix: cdap-site.xml <cdap-site>
    Appendix: cdap-security.xml <cdap-security>


.. |quickstart| replace:: **Quick Start:**
.. _quickstart: quick-start.html

- |quickstart|_ A quick start guide that covers the **most-common case of installing and 
  configuring CDAP.** Many people may find this sufficient; if your case isn't covered, the
  :ref:`install` guide has additional details.


.. |installation| replace:: **Installation and Configuration:**
.. _installation: installation.html

- |installation|_ Covers **installing and configuring CDAP:** the system, network, and software
  requirements; packaging options; and the instructions for installation and verification of the
  CDAP components so they work with your existing Hadoop cluster.


.. |security| replace:: **Security:**
.. _security: security.html

- |security|_ CDAP supports **securing clusters using a perimeter security model.** This section
  describes enabling security, configuring authentication, testing security, and includes 
  an example configuration file.


.. |monitoring| replace:: **Monitoring:**
.. _monitoring: monitoring.html

- |monitoring|_ CDAP collects **logs and metrics** for all of its internal services. 
  This section provides links to the relevant APIs for accessing these logs and metrics.


.. |appendices| replace:: **Appendices:**

- |appendices| Two appendices cover the XML files used to configure the 
  :ref:`CDAP installation <appendix-cdap-site.xml>` and the :ref:`security configuration.
  <appendix-cdap-security.xml>`



.. rubric:: Putting CDAP into Production

The Cask Data Application Platform (CDAP) can be run in different modes: in-memory mode
for unit testing, Standalone CDAP for testing on a developer's laptop, and Distributed
CDAP for staging and production.

Regardless of the runtime edition, CDAP is fully functional and the code you develop never
changes. However, performance and scale are limited when using in-memory or standalone
CDAPs.


.. _in-memory-data-application-platform:

.. rubric:: In-memory CDAP

The in-memory CDAP allows you to easily run CDAP for use in unit tests. In this mode, the
underlying Big Data infrastructure is emulated using in-memory data structures and there
is no persistence. The CDAP Console is not available in this mode.


.. rubric:: Standalone CDAP

The Standalone CDAP allows you to run the entire CDAP stack in a single Java Virtual
Machine on your local machine and includes a local version of the CDAP Console. The
underlying Big Data infrastructure is emulated on top of your local file system. All data
is persisted.

The Standalone CDAP by default binds to the localhost address, and is not available for
remote access by any outside process or application outside of the local machine.

See :ref:`Getting Started <getting-started-index>` and the *Cask Data Application Platform
SDK* for information on how to start and manage your Standalone CDAP.


.. rubric:: Distributed Data Application Platform

The Distributed CDAP runs in fully distributed mode. In addition to the system components
of the CDAP, distributed and highly available deployments of the underlying Hadoop
infrastructure are included. Production applications should always be run on a Distributed
CDAP.

To learn more about getting your own Distributed CDAP, see `Cask Products
<http://cask.co/products>`__.

