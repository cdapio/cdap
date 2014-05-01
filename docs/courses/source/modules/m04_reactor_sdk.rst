=======================
Continuuity Reactor SDK
=======================

.. .. reST Editor: .. section-numbering::
.. reST Editor: .. contents::

.. Slide Presentation HTML Generation
.. landslide: theme ../_theme/slides-generation/
.. landslide: build ../../html/

.. include:: ../_slide-fragments/continuuity_logo.rst

.. |br_m04| raw:: html

   <br />

----

Module Objectives
=================

In this module, you will look at:

- What is the Continuuity Reactor SDK?
- Downloading the Continuuity Reactor SDK
- Installing the SDK
- Exploring the contents of the SDK

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
=============================

- Local Continuuity Reactor Application

  - Scripts for starting, stopping, restarting, status of Reactor
  - Includes Continuuity Reactor Dashboard Web Application
  
- Examples
- Documentation

  - Javadocs
  - REST API Guide
  - Online documentation

----

Local Continuuity Reactor Application Scripts
=============================================

Utility programs located in ``bin/`` for:

- Reactor starting, stopping, restarting status

  - ``reactor.sh`` for \*nix systems
  - ``reactor.bat`` for Windows

- Running examples

----

Continuuity Reactor Dashboard
=============================

- Viewed by starting local Reactor and then
  connecting to ``http://localhost:9999``

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_01_overview.png
   :width: 600px

----

Examples
=============

- Located in both ``examples/`` and online
- ``README.rst`` includes descriptions

----

Javadocs
=============

- Javadocs for Java portion of API
- Located in ``javadocs/``
- REST API documented separately
- Examples & Documentation are best place to start learning

----

REST API Guide
==============
   
- Local copy of REST API located in ``docs/``:|br_m04|
  ``REST-API-Reference-v<version>.pdf``
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

You should now be able to:

- Download the Continuuity Reactor SDK
- Install the SDK
- Find the start scripts for Local Reactor
- Start Local Reactor 
- Start Dashboard webapp and take the Quickstart tour

----

Module Completed
================

`Chapter Index <return.html#m04>`__
