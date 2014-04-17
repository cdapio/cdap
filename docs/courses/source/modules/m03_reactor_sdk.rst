=======================
Continuuity Reactor SDK
=======================

.. include:: ../_slide-fragments/continuuity_logo.rst

----

Module Objectives
=================

In this module, you will look at:

- What is the Continuuity Reactor SDK?
- Downloading the Continuuity Reactor SDK
- Exploring the contents of the SDK
- Additional materials


----

What is the Continuuity Reactor SDK?
====================================

- A software development kit **and** a local Continuuity Reactor
- Software development kit:

  - APIs
  - Examples
  - Documentation
  
- Local Continuuity Reactor

  - Different than distributed version
  - Useful for prototyping and demonstrating
  - Deploy to a distributed Reactor with no code changes

----

How to get the SDK
==================

Download the Software Development Kit

- ``http://www.continuuity.com/download``
- Register for free account
- Download ``continuuity-sdk-<version>.zip``
- Unzip the download

----

Exploring the Contents of SDK
============================

- Local Continuuity Reactor Application

  - Scripts for starting, stopping, status of Reactor
  - Includes Continuuity Reactor Dashboard Web Application
  
- Examples
- Documentation

  - Javadocs
  - REST API Guide
  - Online documentation

----

Local Continuuity Reactor Application
=====================================

- Reactor starting, stopping, status

  - ``reactor.sh`` for \*nix systems
  - ``reactor.bat`` for Windows

- Located in ``bin/``

----

Continuuity Reactor Dashboard
=============================

- Viewed by starting local Reactor and then
  connecting to ``http://localhost:9999``
- Covered in other modules

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_01_overview.png
   :width: 600px

----

Scripts
=======

- Utility programs for:

  - Local Reactor starting, stopping, status
  - Running examples

----

Examples
=============

- 15 examples
- ``README.rst`` includes descriptions
- Located in ``examples/``

----

Javadocs
=============
::

	allclasses-frame.html
	allclasses-noframe.html
	com/
	constant-values.html
	deprecated-list.html
	help-doc.html
	index-all.html
	index.html
	overview-frame.html
	overview-summary.html
	overview-tree.html
	package-list
	resources/
	serialized-form.html
	stylesheet.css

- Javadocs for Java portion of API
- Located in ``javadocs/``
- REST API documented separately
- Examples & Documentation are best place to start learning

----

REST API Guide
==============

- Local copy of REST API located in ``docs/``::

	REST-API-Reference-v<version>.pdf

- All other docs online
- ``http://www.continuuity.com/developers/``

----

Online Documentation
====================

- Introduction
- Examples
- Quickstart
- Programming Guide
- Operations Guide
- Advanced Features
- Javadocs
- REST API
- FAQ

At ``http://www.continuuity.com/developers/``

-----

Module Summary
==============

You have looked at:

- The Continuuity Reactor SDK
- Explored the contents of the SDK
- Looked at what to do with the SDK
- Sources for additional help and materials


