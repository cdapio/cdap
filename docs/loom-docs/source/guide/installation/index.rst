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

.. _prerequisites:

Prerequisites
=============

You'll need Java™, Node.js™ and Ruby™.

The Reactor example apps are pre-compiled, but if you want to modify and compile an app, you'll also need Apache Ant installed on your system as discussed below.

Operating System
----------------
For operating system requirements, please see the :ref:`overview_system-requirements` page

Java
----
JDK or JRE version 6 or 7 must be installed in your environment.

RHEL
^^^^
`Click here <http://www.java.com/en/download/manual.jsp>`_ to download the Java Runtime for Linux and Solaris.

MAC OS
^^^^^^
On Mac OS X, the JVM is bundled with the operating system.

Following installation, please set the JAVA_HOME environment variable.

Ruby
----
Ruby version v1.9.3 or greater is required for Loom. To check for your Ruby installation version, use the command:
::
 ruby -v

RHEL
^^^^
Follow installation instructions found on `this page <https://www.ruby-lang.org/en/installation/>`_.

MAC OS
^^^^^^
Ruby 2.0 is preinstalled on Mac OS X Mavericks. If you are using an older version of Mac, please follow the instructions on `this page <https://www.ruby-lang.org/en/installation/>`_.


Node.js
-------

The version of Node.js must be v0.8.16 or greater.

RHEL
^^^^
For RHEL-based operating systems, consider installing Node.js using RPM:
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




