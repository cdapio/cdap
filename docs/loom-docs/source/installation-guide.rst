.. _overview_toplevel:

==================
Installation Guide
==================

.. _overview:

Overview
========

This document will guide you through the process of installing Continuuity Loom
on your own cluster with the official installation image.

.. _doc_overview:

Prerequisites
=============

You'll need Java™, Node.js™ and Ruby™.

The Reactor example apps are pre-compiled, but if you want to modify and compile an app, you'll also need Apache Ant installed on your system as discussed below.

Operating System
----------------
You'll need blah blah blah Linux distributions.

Java
----
The latest version of the JDK or JRE version 6 great must be installed in your environment.
	•	`Click here <http://www.java.com/en/download/manual.jsp>`_ to download the Java Runtime for Linux and Solaris.
	•	On Mac OS X, the JVM is bundled with the operating system.
	•	Set the JAVA_HOME environment variable after installing Java.

Node.js
-------

The version of Node.js must be v0.8.16 or greater.

MAC OS
^^^^^^
You can download the latest version of Node.js from `their website <http://nodejs.org/>`_ using any of the methods they suggest.

RHEL
^^^^
For RHEL-based operating systems, consider installing Node.js using RPM:
::
 $ wget http://mirrors.xmission.com/fedora/epel/6/i386/epel-release-6-8.noarch.rpm
 $ rpm -i epel-release-6-8.noarch.rpm
 $ yum install npm

Ruby
----
The version of Ruby must be v1.9.3 or greater.

MAC OS
^^^^^^
Ruby is preinstalled on Mac.

RHEL
^^^^
Follow installation instructions found on `this page <https://www.ruby-lang.org/en/installation/>`_


.. _installation:

Installation Guide
==================

Supported Platforms
-------------------

SQLAlchemy has been tested against the following platforms:

* cPython since version 2.6, through the 2.xx series
* cPython version 3, throughout all 3.xx series
* `Pypy <http://pypy.org/>`_ 2.1 or greater

.. versionchanged:: 0.9
   Python 2.6 is now the minimum Python version supported.


