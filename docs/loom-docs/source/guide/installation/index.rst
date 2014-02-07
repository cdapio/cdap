.. _guide_installation_toplevel:
.. include:: /toplevel-links.rst

==================
Installation Guide
==================

.. _overview:

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
 * CentOS 6.2
* **Loom Provisioner**
 * CentOS 6.2
* **Loom UI**
 * CentOS 6.2

Supported Databases
-------------------
 * (Default) Derby
 * MySQL version 5.5 or above
 * SQLite
 * PostgreSQL version 8.4 or above

Supported Zookeeper Versions
----------------------------
 * Apache Zookeeper version 3.4 or above
 * CDH4 or CDH5 Zookeeper
 * HDP1 or HDP2 Zookeeper

Supported Internet Protocols
----------------------------
Loom requires IPv4. IPv6 is currently not supported.

.. _prerequisites:
Software Prerequisites
======================

Loom requires Java™, Node.js™ and Ruby™.

Java
----
JDK or JRE version 6 or 7 must be installed in your environment. Loom is supported with Oracle JDK. For JDK 1.6, Loom is certified with 1.6.0_31.

Linux
^^^^^
`Click here <http://www.java.com/en/download/manual.jsp>`_ to download the Java Runtime for Linux and Solaris. Following installation, please set the JAVA_HOME environment variable.

Mac OS
^^^^^^
On Mac OS X, the JVM is bundled with the operating system. Following installation, please set the JAVA_HOME environment variable.

Ruby
----
Ruby version v1.9.3 or greater is required for Loom. To check for your Ruby installation version, use the command:
::
 ruby -v

Linux
^^^^^
Follow installation instructions found on `this page <https://www.ruby-lang.org/en/installation/>`_.

MAC OS
^^^^^^
Ruby 2.0 is preinstalled on Mac OS X Mavericks. If you are using an older version of Mac OS, please follow the instructions on `this page <https://www.ruby-lang.org/en/installation/>`_.

Node.js
-------

The version of Node.js must be v0.8.16 or greater.

Linux
^^^^^
For Linux-based operating systems, consider installing Node.js using RPM:
::
 $ wget http://mirrors.xmission.com/fedora/epel/6/i386/epel-release-6-8.noarch.rpm
 $ rpm -i epel-release-6-8.noarch.rpm
 $ yum install npm

MAC OS
^^^^^^
You can download the latest version of Node.js from `their website <http://nodejs.org/>`_ using any of the methods they suggest.


.. _setting-environmental-variables:

Setting Environmental Variables
===============================


.. _installation:
Installing from Package
=======================


.. _common-issues:
Common Installation Issues
==========================




