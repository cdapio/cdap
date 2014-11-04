.. meta::
    :author: Cask Data, Inc.
    :description: Index document
    :copyright: Copyright © 2014 Cask Data, Inc.

:showtoc: true

.. _getting-started-index:

============================================
Getting Started with CDAP
============================================

.. toctree::
   :maxdepth: 1
   
   System Requirements <system>
   Quick Start <quick-start>
   Standalone Setup <standalone/index>
   Development Environment Setup <dev-env>
   Starting and Stopping CDAP <start-stop-cdap>
   Building and Running Applications <building-apps>


The `CDAP Software Development Kit (SDK) <http://cask.co/downloads/#cdap>`__ is all that
is needed to develop CDAP applications in your development environment, either your laptop
or a workstation. It includes:

- A :ref:`Standalone CDAP <standalone_index>` that can run on a single machine in a single JVM. It provides all of
  the CDAP APIs without requiring a Hadoop cluster, using alternative, fully-functional
  implementations of CDAP features. For example, application containers are implemented as
  Java threads instead of YARN containers.
- The :ref:`CDAP Console, <cdap-console>` a web-based graphical user interface to interact with CDAP instances
  and the applications they run.
- Tools for :ref:`ingesting data <ingesting-data>` and :ref:`authenticating
  clients <authentication-clients>`, :ref:`datasets, <datasets-index>` and :ref:`example
  applications <examples-index>` to help you become familiar with CDAP, perform common
  tasks, and serve as templates for developing your own applications.
  
Online, we have many additional resources, ranging from an extensive set of guides
(:ref:`Developers’ Guide <developer-index>`, 
:ref:`Reference <reference:reference-index>`, 
:ref:`Administration <admin:admin-index>`,
:ref:`Examples, How-To Guides, and Tutorials <examples:examples-introduction-index>`)
to :ref:`user groups. <faq-cdap-user-groups>`
