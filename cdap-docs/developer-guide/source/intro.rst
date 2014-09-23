.. :author: Cask Data, Inc.
   :description: Introduction to the Cask Data Application Platform
   :copyright: Copyright © 2014 Cask Data, Inc.

==================================================
Introduction to the Cask Data Application Platform
==================================================

The Cask |(TM)| Data Application Platform (CDAP) provides virtualization for data and applications
on Hadoop.

- Data virtualization is through CDAP's dataset framework, which allows you to write
  your application agnostic to the representation of data in actual storage engines, and to
  encapsulate your applications data access patterns in reusable libraries.
- Application virtualization is achieved by providing different runtimes for various
  environments. Be it in memory for unit testing, a standalone runtime that runs on a
  single computer, or a distributed runtime with execution in the YARN containers of a
  Hadoop cluster. The application can be written independent of where it is executed.

This document is your complete guide to the Cask Data Application Platform: It helps you get
started and set up your development environment; it explains how CDAP works and teaches how
develop and test application on top of CDAP, or how to virtualize an existing Hadoop application,
and how to install, monitor and diagnose a fully distributed CDAP in a Hadoop cluster. It also
contains a complete reference of CDAP programming APIs and client interfaces.


Where to Go Next
================
Now that you've had an introduction to CDAP, take a look at:

- `Cask Data Application Platform Getting Started Guide <getstarted.html>`__,
  which guides you through installing CDAP, setting up your development environment, and building and running
  an example application.

.. |(TM)| unicode:: U+2122 .. trademark sign
   :trim:
