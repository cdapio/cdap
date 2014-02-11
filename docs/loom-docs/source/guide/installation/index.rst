.. _guide_installation_toplevel:

.. index::
   single: Installation Guide
==================
Installation Guide
==================


Overview
========

This document will guide you through the process of installing Continuuity Loom
on your own cluster with the official installation image.

System Requirements
===================

.. _system-requirements:

Supported Operating System
--------------------------

Various systems of Loom have been tested against the following platforms:

* **Loom Server**
 * CentOS 6.4
 * Ubuntu 12.04
* **Loom Provisioner**
 * CentOS 6.4
 * Ubuntu 12.04
* **Loom UI**
 * CentOS 6.4
 * Ubuntu 12.04

Supported Databases
-------------------
 * (Default) Derby
 * MySQL version 5.1 or above
 * Oracle DB
 * SQLite
 * PostgreSQL version 8.4 or above

Supported Zookeeper Versions
----------------------------
 * Apache Zookeeper version 3.4 or above
 * CDH4 or CDH5 Zookeeper
 * HDP1 or HDP2 Zookeeper

Supported Internet Protocols
----------------------------
Loom requires IPv4. IPv6 is currently not supported

.. _prerequisites:
Software Prerequisites
======================

Loom requires Java™. JDK or JRE version 6 or 7 must be installed in your environment. Loom is supported with Oracle JDK. For JDK 1.6, Loom is certified with 1.6.0_31.

Linux
^^^^^
`Click here <http://www.java.com/en/download/manual.jsp>`_ to download the Java Runtime for Linux and Solaris. Following installation, please set the ``JAVA_HOME`` environment variable.

Mac OS
^^^^^^
On Mac OS X, the JVM is bundled with the operating system. Following installation, please set the ``JAVA_HOME`` environment variable.

.. _setting-environmental-variables:
Setting Environmental Variables
===============================

Several environmental variables can be set in Loom Provisioner and Loom UI.


Loom Provisioner
----------------
The Provisioner environmental variables can be set at ``/etc/default/loom-provisioner``. The configurable variables are as below:

.. list-table::

   * - Variable
     - Default
     - Description
   * - ``LOOM_NUM_WORKERS``
     - 5
     - The number of provisioner workers spawned
   * - ``LOOM_LOG_DIR``
     - /var/log/loom
     - Path for the log directory
   * - ``LOOM_SERVER_URI``
     - http://localhost:55054
     - The URI for Loom Server


Loom UI
-------
The UI environmental variables can be set at ``/etc/default/loom-ui``. The configurable variables are as below:

.. list-table::

   * - Variable
     - Default
     - Description
   * - ``LOOM_LOG_DIR``
     - /var/log/loom
     - Path for the log directory
   * - ``LOOM_SERVER_URI``
     - http://localhost:55054
     - The URI for Loom Server

.. _installation-file:
Installing from File
====================

Yum
---
To install each of the Loom components locally from a Yum package:
::
  # Loom Server
  yum localinstall loom-server-0.1.1-1.el6.x86_64.rpm

  # Loom Provisioner
  yum localinstall loom-provisioner-0.1.1-1.el6.x86_64.rpm

  # Loom UI
  yum localinstall loom-ui-0.1.1-1.el6.x86_64.rpm


Debian
------
To install each of the Loom components locally from a Debian package:
::
  # Loom Server
  dpkg -i loom-server_0.1.1-1.ubuntu.12.04_amd64.deb

  # Loom Provisioner
  dpkg -i loom-provisioner_0.1.1-1.ubuntu.12.04_amd64.deb

  # Loom UI
  dpkg -i loom-ui_0.1.1-1.ubuntu.12.04_amd64.deb

.. _installation-repository:
Installing from Repository
==========================

Access to the Continuuity private repository is required for package installation.

Yum
---
To add the Continuuity Yum repository, add the following content to the file ``/etc/yum.repos.d/continuuity.repo``:
::
  [continuuity]
  name=Continuuity Releases
  baseurl=https://<username>:<password>@repository.continuuity.com/content/groups/restricted
  enabled=1
  protect=0
  gpgcheck=0
  metadata_expire=30s
  autorefresh=1
  type=rpm-md

.. note:: username and password should be url encoded

Instructions for installing each of the Loom components are as below:
::
  # Loom Server
  yum install loom-server

  #Loom Provisioner
  yum install loom-provisioner

  #Loom UI
  yum install loom-ui

Debian
------
To add the Continuuity Debian repository, add the following content to the file ``/etc/apt/sources.list.d/continuuity.list``:
::
  deb     [arch=amd64] https://<username>:<password>@repository.continuuity.com/content/sites/apt precise release

Instructions for installing each of the Loom components are as below:
::
  # Loom Server
  apt-get install loom-server

  #Loom Provisioner
  apt-get install loom-provisioner

  #Loom UI
  apt-get install loom-ui

Database Configuration
----------------------
By default, Loom uses an embedded Derby database. However, you can optionally choose to enable remote database for Loom server.
Additional steps are required to configure this setting.

**Download and add the database connector JAR**

Execute the following command on the Loom server machine:

For RHEL/CentOS/Oracle Linux:
::
  yum install mysql-connector-java*
For SLES:
::
  zypper install mysql-connector-java*
For Ubuntu:
::
  apt-get install mysql-connector-java*

After the install, the MySQL JAR is placed in ``/usr/share/java/``. Copy the downloaded JAR file to the
``/opt/loom/server/lib/`` directory on your Loom server machine. Verify that the JAR file has appropriate permissions.

You will need to set up an account and a database in MySQL. An example schema file (for MySQL) for this can be found at
``/opt/loom/server/docs/sql``.

.. note:: after installing the mysql connector, the java version may change.  Make sure you are using java 1.6 from oracle.  You may need to run update-alternatives --config java to do this.

Loom server Configuration
-------------------------

Loom server settings can be changed under the ``/etc/loom/conf/loom-site.xml`` configuration file. For a list of
available configurations, see the :doc:`Server Configuration </guide/admin/server-config>` page.

.. _common-issues:
Common Installation Issues
==========================

A common issue is installing Loom on machines that have Open JDK installed rather than Oracle JDK.
Loom currently does not support Open JDK.





