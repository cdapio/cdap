.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

:hide-toc: true

.. _installation-index:

============
Installation
============

.. toctree::
   :maxdepth: 1
   
    Quick Start <quick-start>
    Installation <installation>
    Configuration <configuration>
    Security <security>
    Appendix: cdap-site.xml <cdap-site>
    Appendix: cdap-security.xml <cdap-security>

  
.. include:: installation-note.txt  

Covers **putting CDAP into production, with installation, configuration, and security setup.**
Appendices cover the XML files used to configure the CDAP installation and security configurations.

.. |quickstart| replace:: **Quick Start:**
.. _quickstart: quick-start.html

- |quickstart|_ A quick start guide that covers the **most-common case of installing and 
  configuring CDAP.** Many people may find this sufficient; if your case isn't covered, the
  :ref:`install` and :ref:`configuration` guides have additional details. Use this if you aren't using 
  :ref:`Cloudera Manager <cloudera-configuring>` or :ref:`Apache Ambari <ambari-configuring>`.


.. |installation| replace:: **Installation:**
.. _installation: installation.html

- |installation|_ Covers **installing CDAP:** the system, network, and software
  requirements; packaging options; and the instructions for installation of the
  CDAP components so they will work with your existing Hadoop cluster. Use this if you aren't
  using the :ref:`Quick Start guide <installation-quick-start>`, 
  :ref:`Cloudera Manager <cloudera-configuring>`, or :ref:`Apache Ambari <ambari-configuring>`.


.. |configuration| replace:: **Configuration:**
.. _configuration: configuration.html

- |configuration|_ Covers **configuring CDAP:** once CDAP :ref:`is installed <install>`,
  covers the configuration and verification of the CDAP components.


.. |security| replace:: **Security:**
.. _security: security.html

- |security|_ CDAP supports **securing clusters using a perimeter security model.** This section
  describes enabling security, configuring authentication, testing security, and includes 
  an example configuration file.


.. |appendices| replace:: **Appendices:**

- |appendices| Two appendices cover the XML files used to configure the 
  :ref:`CDAP installation <appendix-cdap-site.xml>` and the :ref:`security configuration.
  <appendix-cdap-security.xml>`


.. _modes-data-application-platform:

.. rubric:: Putting CDAP into Production

The Cask Data Application Platform (CDAP) can be run in different modes: **In-memory mode**
for unit testing and continuous integration pipelines, **Standalone CDAP** for testing on a
developer's laptop, and **Distributed CDAP** for staging and production.

Regardless of the runtime mode, CDAP is fully-functional and the code you develop never
changes. However, performance and scale are limited when using in-memory or standalone
CDAPs. CDAP Applications are developed against the CDAP APIs, making the switch between
these modes seamless. An application developed using a given mode can easily run in
another mode.


.. _in-memory-data-application-platform:

.. rubric:: In-memory CDAP

The in-memory CDAP allows you to easily run CDAP for use in unit tests and continuous
integration (CI) pipelines. In this mode, the underlying Big Data infrastructure is
emulated using in-memory data structures and there is no persistence. The CDAP UI is not
available in this mode. See :ref:`test-cdap` for information and examples on using this
mode.

- Purpose-built for writing unit tests and CI pipelines
- Mimics storage technologies as in-memory data structures; for example, Java NavigableMap
- Uses Java Threads as the processing abstraction (via Apache Twill)


.. _standalone-data-application-platform:

.. rubric:: Standalone CDAP

The Standalone CDAP allows you to run the entire CDAP stack in a single Java Virtual
Machine on your local machine and includes a local version of the CDAP UI. The
underlying Big Data infrastructure is emulated on top of your local file system. All data
is persisted.

The Standalone CDAP by default binds to the localhost address, and is not available for
remote access by any outside process or application outside of the local machine.

See :ref:`Getting Started <getting-started-index>` and the *Cask Data Application Platform
SDK* for information on how to start and manage your Standalone CDAP.

- Designed to run in a standalone environment, for development and testing
- Uses LevelDB/Local File System as the storage technology
- Uses Java Threads as the processing abstraction (via Apache Twill)

.. _distributed-data-application-platform:

.. rubric:: Distributed Data Application Platform

The Distributed CDAP runs in fully distributed mode. In addition to the system components
of the CDAP, distributed and highly available deployments of the underlying Hadoop
infrastructure are included. Production applications should always be run on a Distributed
CDAP.

- Production, Staging, and QA mode; runs in a distributed environment
- Uses Apache HBase and HDFS as the storage technology (today)
- Uses Apache Yarn Containers as the processing abstraction (via Apache Twill)
