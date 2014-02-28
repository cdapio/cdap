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

Supported OpenStack Versions
----------------------------
Loom has been extensively tested on Havana, but it also supports Grizzly out of the box. 

.. note:: Click here for more information on how :doc:`Openstack should be configured <openstack-config>` currently to support provisioning with Loom. Several limitations that exists will be eliminated in future releases of Loom.

Supported Internet Protocols
----------------------------
Loom requires IPv4. IPv6 is currently not supported.

Supported Browsers
------------------
 * Mozilla Firefox version 26 or above
 * Google Chrome version 31 or above
 * Safari version 5.1 or above

.. _prerequisites:
Software Prerequisites
======================

Loom requires Java™. JDK or JRE version 6 or 7 must be installed in your environment. Loom is certified with Oracle JDK 6.0_31, Oracle JDK 7.0_51 and OpenJDK 6b27-1.12.6.

Linux
^^^^^
`Click here <http://www.java.com/en/download/manual.jsp>`_ to download the Java Runtime for Linux and Solaris. Following installation, please set the ``JAVA_HOME`` environment variable.

Mac OS
^^^^^^
On Mac OS X, the JVM is bundled with the operating system. Following installation, please set the ``JAVA_HOME`` environment variable.

.. _installation-file:
Installing from File
====================

.. note:: Installation of Loom packages creates a user with the username 'loom'. If the user 'loom' already exists on the system, then that user account will be used to run all Loom services. The username can also be externally created using LDAP.

Yum
---
To install each of the Loom components locally from a Yum package:

.. parsed-literal::
  $ sudo yum localinstall loom-server-\ |version|\ .el6.x86_64.rpm
  $ sudo yum localinstall loom-provisioner-\ |version|\ .el6.x86_64.rpm
  $ sudo yum localinstall loom-ui-\ |version|\ .el6.x86_64.rpm


Debian
------
To install each of the Loom components locally from a Debian package:

.. parsed-literal::
  $ sudo dpkg -i loom-server\_\ |version|\ .ubuntu.12.04_amd64.deb
  $ sudo dpkg -i loom-provisioner\_\ |version|\ .ubuntu.12.04_amd64.deb
  $ sudo dpkg -i loom-ui\_\ |version|\ .ubuntu.12.04_amd64.deb

.. _installation-repository:
Installing from Repository
==========================

Access to the Continuuity private repository is required for package installation.

Yum
---
To add the Continuuity Yum repository, add the following content to the file ``/etc/yum.repos.d/continuuity.repo``:
::
  [continuuity]
  name=Continuuity Loom Releases
  baseurl=https://<username>:<password>@repository.continuuity.com/content/repositories/loom
  enabled=1
  protect=0
  gpgcheck=0
  metadata_expire=30s
  autorefresh=1
  type=rpm-md

.. note:: Username and password are URL encoded. Please request login credentials from Continuuity support.

Instructions for installing each of the Loom components are as below:
::
  $ sudo yum install loom-server
  $ sudo yum install loom-provisioner
  $ sudo yum install loom-ui

Debian
------
To add the Continuuity Debian repository, add the following content to the file ``/etc/apt/sources.list.d/continuuity.list``:
::
  deb     [arch=amd64] https://<username>:<password>@repository.continuuity.com/content/sites/apt-loom precise release

Instructions for installing each of the Loom components are as below:
::
  $ sudo apt-get update
  $ sudo apt-get install loom-server
  $ sudo apt-get install loom-provisioner
  $ sudo apt-get install loom-ui

Database Configuration
----------------------
By default, Loom uses an embedded Derby database. However, you can optionally choose to enable remote database for Loom server.
Additional steps are required to configure this setting.

Sample MySQL setup
^^^^^^^^^^^^^^^^^^
**Download and add the database connector JAR**

Execute the following command on the Loom server machine:

For RHEL/CentOS/Oracle Linux:
::
  $ sudo yum install mysql-connector-java*
For Ubuntu:
::
  $ sudo apt-get install libmysql-java*

After the install, the MySQL JAR is placed in ``/usr/share/java/``. Copy the installed JAR files to the
``/opt/loom/server/lib/`` directory on your Loom server machine. Verify that the JAR file has appropriate permissions.

.. note::
  * After installing the MySQL connector, the Java version may change.  Make sure you are using Java 1.6 or 1.7 from Oracle.  You may need to run ``update-alternatives --config java`` to do this.
  * The minimum required version of MySQL connector is 5.1.6.
  * You can also download MySQL JDBC driver JAR (mysql-connector-java) from `MySQL website <http://dev.mysql.com/downloads/connector/j>`_.

**Setup database**

You will need to set up an account and a database in MySQL. An example schema file (for MySQL) for this can be found at
``/opt/loom/server/docs/sql``.

If you are setting up a MySQL database from scratch you can run the following on your mysql machine to complete the database setup:

.. parsed-literal::
  $ mysql -u root -p<root-password> -e 'create database loom;'
  $ mysql -u root -p<root-password> -e 'grant all privileges on loom.* to "loom"@"<loom-server>" identified by "<password>";'
  $ mysql -u loom -p<password> loom < /opt/loom/server/docs/sql/loom-\ |version|\ -create-tables-mysql.sql
  $ mysql -u loom -p<password> loom -e 'show tables;'
  +----------------+
  | Tables_in_loom |
  +----------------+
  | clusters       |
  | jobs           |
  | nodes          |
  | tasks          |
  +----------------+

where loom.sql is the example schema file at ``/opt/loom/server/docs/sql``, and where passwords are replaced as needed.

Loom Server Configuration
-------------------------

Loom server settings can be changed under the ``/etc/loom/conf/loom-site.xml`` configuration file. For a list of
available configurations, see the :doc:`Server Configuration </guide/admin/server-config>` page.


.. _setting-environmental-variables:
Setting Environmental Variables
===============================

Several environmental variables can be set in Loom Provisioner and Loom UI.

Loom Server
-----------
The Server environmental variables can be set at ``/etc/default/loom-server``. The configurable variables are as below:

.. list-table::
   :header-rows: 1

   * - Variable
     - Default
     - Description
   * - ``LOOM_LOG_DIR``
     - /var/log/loom
     - Path for the log directory
   * - ``LOOM_JMX_OPTS``
     -
     - JMX options for monitoring the Loom Server
   * - ``LOOM_GC_OPTS``
     -
     - java garbage collection options to use when running the Loom Server
   * - ``LOOM_JAVA_OPTS``
     - -XX:+UseConcMarkSweepGC -XX:+UseParNewGC
     - java options to use when running the Loom Server

Loom Provisioner
----------------
The Provisioner environmental variables can be set at ``/etc/default/loom-provisioner``. The configurable variables are as below:

.. list-table::
   :header-rows: 1

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
   * - ``LOOM_LOG_LEVEL``
     - info
     - Logging level


Loom UI
-------
The UI environmental variables can be set at ``/etc/default/loom-ui``. The configurable variables are as below:

.. list-table::
   :header-rows: 1

   * - Variable
     - Default
     - Description
   * - ``LOOM_LOG_DIR``
     - /var/log/loom
     - Path for the log directory
   * - ``LOOM_SERVER_URI``
     - http://localhost:55054
     - The URI for Loom Server
   * - ``LOOM_UI_PORT``
     - 8100
     - The port number that hosts the UI

.. _starting_stopping:
Starting and Stopping Loom Services
===================================
By default, the Loom's installation RPMs and PKGs do not configure auto start of the services in the ``init.d``. We leave
that privilege to the administrator. For each Loom component and its related service (such as the Server, Provisioner, and UI),
there is a launch script, which you may use to execute a desired operation. For example, to start, stop, or check status
for a Loom Provisioner, you can use:
::
  $ sudo /etc/init.d/loom-server start|stop
  $ sudo /etc/init.d/loom-provisioner start|stop|status
  $ sudo /etc/init.d/loom-ui start|stop

.. _loading_defaults:
Loading Default Templates
=========================

Loom provides a set of useful default templates that covers most supported use cases. For new users and administrators of Loom, we
recommend installing these defaults as a starting point for template definition. These defaults are required for running
the example in the :doc:`Quick Start Guide </guide/quickstart/index>`. To load these templates, run:
::
  $ export LOOM_SERVER_URI=http://<loom-server>:<loom-port>/v1/loom
  $ /opt/loom/server/docs/defaults/load-defaults.sh

.. note::
    Setting the ``LOOM_SERVER_URI`` environment variable is only required if you have configured the Loom Server to
    bind to an address other than localhost.

.. _logs:
Log Management
==============

Location
--------
By default, Loom logs are located at ``/var/log/loom``.  This can be changed by editing the corresponding ``/etc/default/loom-server``,
``/etc/default/loom-ui``, or ``/etc/default/loom-provisioner`` file.

Options
-------
Log options for the server, such as log level, can be changed by editing the ``/etc/loom/conf/logback.xml`` file.  Log level for
the provisioner can be changed by editing the ``/etc/default/loom-provisioner`` file.

Rotation
--------
Loom depends on the external Linux utility logrotate to rotate its logs. Loom
packages contain logrotate configurations in ``/etc/logrotate.d`` but it does not perform the rotations itself.
Please ensure logrotate is enabled on your Loom hosts.

.. _common-issues:
Common Installation Issues
==========================

* A common issue is installing Loom on machines that have Open JDK installed rather than Oracle JDK. Loom currently does not support Open JDK.

* If you see JDBC exceptions in the Loom Server log like:
  ::
    Caused by: java.lang.AbstractMethodError: com.mysql.jdbc.PreparedStatement.setBlob(ILjava/io/InputStream;)

  it means your JDBC connector version is too old.  Upgrade to a newer version to solve the problem.

* If you are running your mysql server on the same machine as the Loom Server and are seeing connection issues in the Loom Server logs, you may need to explicitly grant access to "loom"@"localhost" instead of relying on the wildcard. 
