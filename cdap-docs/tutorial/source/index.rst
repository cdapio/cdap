.. meta::
    :author: Cask Data, Inc.
    :description: Introduction to the Cask Data Application Platform
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _tutorial-intro:

===============================================================
Introduction to the Cask Data Application Platform v\ |version|
===============================================================

.. include:: ../../_common/_source/index.rst 
   :start-after: .. rubric:: Introduction to the Cask Data Application Platform
   :end-before:  These documents are your complete reference


.. From "CDAP Software Development Kit (SDK)":
.. System Requirements and Dependencies, Download and Setup

.. include:: ../../developers-manual/source/getting-started/standalone/index.rst 
   :start-after: .. _system-requirements:
   :end-before: .. _standalone-setup:


.. rubric:: Standalone CDAP Download, Installation and Setup

There are three ways to `download <http://cask.co/downloads/#cdap>`__ and install the CDAP SDK: 

- as a :doclink:`binary zip file <developers-manual/getting-started/standalone/zip.html>`;
- as a :doclink:`Virtual Machine image <developers-manual/getting-started/standalone/virtual-machine.html>`; or 
- as a :doclink:`Docker image <developers-manual/getting-started/standalone/docker.html>`.

If you already have a :ref:`development environment
<dev-env>` setup, the :doclink:`zip file
<developers-manual/getting-started/standalone/zip.html>` is your easiest solution.

If you don't have a development environment, the :doclink:`Virtual Machine image
<developers-manual/getting-started/standalone/virtual-machine.html>` offers a
pre-configured environment with CDAP pre-installed and that automatically starts
applications so that you can be productive immediately. You can build your own projects or
follow the provided example applications.

The :doclink:`Docker image  <developers-manual/getting-started/standalone/docker.html>` is
intended for those developing on Linux.

Follow one of the above links for download and installation instructions.


.. rubric:: CDAP Tutorials

Two tutorials are currently available to get you started with running and developing CDAP Applications:

.. |basic| replace:: **Basic Tutorial:**
.. _basic: tutorial1.html

- |basic|_ A Simple Web Analytics Application

  This tutorial provides the basic steps for the development of a data application using the
  Cask Data Application Platform (CDAP). We will use a simple Web Analytics Application to
  demonstrate how to develop with CDAP and how CDAP helps when building data applications
  that run in the Hadoop ecosystem.


.. |advanced| replace:: **Advanced Tutorial:**
.. _advanced: tutorial2.html

- |advanced|_ Wise (Web Insights Engine Application)

  Performing advanced real-time or batch processing of analytics on a Web application
  using access logs can be a demanding application. Using the **Web Insights Engine
  Application** or *Wise*, we’ll show you how to build a system on CDAP that is easy,
  concise, and powerful. Wise extracts value from Web server access logs, counts visits
  made by different IP addresses seen in the logs in real-time, and computes the bounce
  ratio of each web page encountered using batch processing.

Once you have completed these tutorials, we have additional 
:doclink:`examples <examples-manual/examples/index.html>`, 
:doclink:`guides <examples-manual/how-to-guides/index.html>`, and
:doclink:`applications <examples-manual/apps-packs.html>`,
all accessible through our :doclink:`documentation site <index.html>`.
