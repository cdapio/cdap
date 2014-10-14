.. :author: Cask Data, Inc.
   :description: Index document
   :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Getting Started with CDAP
============================================

.. toctree::
   :maxdepth: 1
   
   System Requirements <system>
   Quick Start <quick-start>
   Standalone Setup <standalone/index>
   Development Environment Setup <dev-env>
   Testing Applications <testing-apps>
   Apps and Packs <apps-packs>
   Creating a New App <create-app>
   Creating a New Pack <create-pack>
   Additional Examples <examples/index>

The CDAP Software Development Kit (SDK) is all that is needed to develop CDAP applications
in your development environment, either your laptop or a work station. It includes:

- A Standalone CDAP that can run on a single machine in a single JVM. It provides all of
  the CDAP APIs without requiring a Hadoop cluster, using alternative, fully functional
  implementations of CDAP features. For example, application containers are implemented as
  Java threads instead of YARN containers.
- The CDAP Console, a web-based graphical user interface to interact with the Standalone CDAP
  and the applications it runs.
- The complete CDAP documentation, including this document and the Javadocs for the CDAP APIs.
- A set of tools, datasets and example applications that help you get familiar with CDAP, and
  can also serve as templates for developing your own applications.
