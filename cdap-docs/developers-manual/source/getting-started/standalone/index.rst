.. meta::
    :author: Cask Data, Inc.
    :description: Index document
    :copyright: Copyright © 2014 Cask Data, Inc.

:hide-toc: true

.. _standalone-index:

============================================
CDAP Software Development Kit (SDK)
============================================

.. toctree::
   :maxdepth: 1
   
   Binary Zip File <zip>
   Virtual Machine Image <virtual-machine>
   Docker Image <docker>


.. _system-requirements:

.. rubric:: System Requirements and Dependencies

The CDAP SDK runs on Linux, MacOS and Windows, and has three requirements:

- `JDK 6 or JDK 7 <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`__ 
  (required to run CDAP; note that $JAVA_HOME should be set)
- `Node.js 0.8.16 through 0.10.37 <http://nodejs.org/dist/>`__ (required to run the CDAP Console UI)
- `Apache Maven 3.0+ <http://maven.apache.org>`__ (required to build CDAP applications)

We recommend using an IDE when building CDAP applications, such as either `IntelliJ
<https://www.jetbrains.com/idea/>`__ or `Eclipse, <https://www.eclipse.org/>`__ as
described in the section on :ref:`development environment setup. <dev-env>`

.. _standalone-setup:

.. rubric:: Standalone CDAP Download, Installation and Setup

There are three ways to `download <http://cask.co/downloads/#cdap>`__ and install the CDAP SDK: 

- as a :doc:`binary zip file <zip>`;
- as a :doc:`Virtual Machine image <virtual-machine>`; or 
- as a :doc:`Docker image <docker>`.

If you already have a :ref:`development environment<dev-env>` setup, the :doc:`zip file <zip>`
is your easiest solution.

If you don't have a development environment, the :doc:`Virtual Machine image <virtual-machine>`
offers a pre-configured environment with CDAP pre-installed and that automatically starts
applications so that you can be productive immediately. You can build your own projects or
follow the provided example applications.

The :doc:`Docker image <docker>` is intended for those developing on Linux.

Follow one of the above links for download and installation instructions.
