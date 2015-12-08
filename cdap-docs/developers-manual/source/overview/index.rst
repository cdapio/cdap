.. meta::
    :author: Cask Data, Inc.
    :description: Architecture of the Cask Data Application Platform
    :copyright: Copyright © 2014 Cask Data, Inc.

:hide-toc: true

.. _cdap-overview:

=======================================
Cask Data Application Platform Overview
=======================================

.. toctree::
   :maxdepth: 1
   
    Anatomy of a Big Data Application <anatomy>
    Modes and Components <modes>
    Abstractions <abstractions>
    Programming Interfaces <interfaces>


**Cask Data Application Platform (CDAP)** is developer-centric middleware for developing and running
Big Data applications. Before you learn how to develop and operate applications, this chapter will
explain the concepts and architecture of CDAP.


.. |anatomy| replace:: **Anatomy of a Big Data Application:**
.. _anatomy: anatomy.html

- |anatomy|_ Explains the **areas of concern in developing a Big Data application,** and how you use CDAP to address these.

.. |modes| replace:: **Modes and Components:**
.. _modes: modes.html

- |modes|_ Covers the **modes** and **components** of CDAP.

.. |abstractions| replace:: **CDAP Abstractions:**
.. _abstractions: abstractions.html

- |abstractions|_ Describes how CDAP abstractions provide **portability by decoupling your
  data and applications** from the underlying infrastructure.

.. |interfaces| replace:: **Programming Interfaces:**
.. _interfaces: interfaces.html

- |interfaces|_ CDAP interfaces can be described as either **Developer or Clients interfaces.**


.. rubric:: Where to Go Next

Now that you've seen the concepts and the architecture of CDAP, you are ready to start writing an application.

Here are some places to start:

- If you haven't already, go through our :ref:`Quick Start example <quick-start>`, which guides you
  through the development and the components of a complete web log analytics application; 

- There are additional :ref:`examples, <examples-index>` :ref:`how-to guides, <guides-index>` and
  :ref:`tutorials <tutorials>` on building CDAP applications; and
  
- :ref:`building-blocks` describes in detail the components of CDAP, and how they interact.
