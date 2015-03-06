.. meta::
    :author: Cask Data, Inc.
    :description: Index document
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

:hide-toc: true

.. _getting-started-index:

============================================
Getting Started with CDAP
============================================

.. toctree::
   :maxdepth: 1
   
   CDAP SDK <standalone/index>
   Quick Start <quick-start>
   Development Environment Setup <dev-env>
   Starting and Stopping CDAP <start-stop-cdap>
   Building and Running Applications <building-apps>


The :ref:`CDAP Software Development Kit (SDK) <standalone-index>` includes all that
is needed to develop CDAP applications in your development environment, either your laptop
or a workstation. It has:

- A :ref:`Standalone CDAP <standalone-index>` that can run on a single machine in a single JVM. It provides all of
  the CDAP APIs without requiring a Hadoop cluster, using alternative, fully-functional
  implementations of CDAP features. For example, application containers are implemented as
  Java threads instead of YARN containers.
- The :ref:`CDAP Console, <cdap-console>` a web-based graphical user interface to interact with CDAP instances
  and the applications they run.
- Tools for :ref:`ingesting data <ingesting-data>` and :ref:`authenticating
  clients <authentication-clients>`, :ref:`datasets, <datasets-index>` and :ref:`example
  applications <examples-index>` to help you become familiar with CDAP, perform common
  tasks, and serve as templates for developing your own applications.

Follow these steps:

1. Make sure you have the :ref:`system requirements and dependencies <system-requirements>`.
#. Download the CDAP SDK, using :ref:`one of the versions <standalone-setup>`.
#. Follow the :ref:`installation instructions <standalone-setup>` for the version you downloaded.
#. To try out an application, follow our :ref:`Quick Start <quick-start>`.
#. We suggest the :ref:`Examples, Guides, and Tutorials <examples:examples-introduction-index>` 
   as the easiest way to become familiar with CDAP.
#. To begin writing your own application, start by setting up your :ref:`development environment <dev-env>`.
#. There are instructions for :doc:`starting and stopping CDAP <start-stop-cdap>`, 
   and :doc:`building and running examples and applications <building-apps>`.

Online, we have additional resources ranging from :ref:`user groups <faq-cdap-user-groups>` to these manuals,
examples, guides, and tutorials:

- :ref:`Developers’ Manual: <developer-index>` Getting Started and Writing Applications with CDAP
- :ref:`Reference Manual: <reference:reference-index>` APIs, Licenses and Dependencies
- :ref:`Administration Manual: <admin:admin-index>` Installation and Operation of Distributed CDAP installations
- :ref:`Examples, How-To Guides, and Tutorials <examples:examples-introduction-index>`