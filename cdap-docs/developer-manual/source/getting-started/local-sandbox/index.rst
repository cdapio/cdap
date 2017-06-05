.. meta::
    :author: Cask Data, Inc.
    :description: CDAP Sandbox
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

:hide-toc: true

.. _local-sandbox-index:
.. _local-sandbox:

==================
CDAP Sandbox
==================

.. toctree::
   :maxdepth: 1

   Binary Zip File <zip>
   Virtual Machine Image <virtual-machine>
   Docker Image <docker>


.. _system-requirements:

.. rubric:: System Requirements and Dependencies

The CDAP Sandbox runs on Linux, MacOS, and Windows, and has these
requirements:

- `JDK 7 or 8 <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`__
  (required to run CDAP; note that $JAVA_HOME should be set)
- `Node.js <https://nodejs.org>`__ (required to run the CDAP UI; we recommend any
  version beginning with |node-js-min-version|.
  Different versions of Node.js `are available <https://nodejs.org/dist/>`__.)
- `Apache Maven 3.0+ <http://maven.apache.org>`__ (required to build CDAP applications)

If you are **running under Microsoft Windows**, you will need to have installed the
`Microsoft Visual C++ 2010 Redistributable Package
<http://www.microsoft.com/en-us/download/details.aspx?id=14632>`__ in order to have the
required DLLs to run Hadoop and CDAP; currently, CDAP is supported only on 64-bit Windows
platforms.

.. include:: /_includes/windows-note.txt

.. _developer-manual-install-node.js:

.. rubric:: Node.js Runtime

You can download an appropriate version of Node.js from `nodejs.org
<https://nodejs.org>`__. We recommend any version of `Node.js <https://nodejs.org/>`__
beginning with |node-js-min-version|.

.. We support Node.js up to |node-js-max-version|.

You can check if ``node.js`` is installed, in your path, and an appropriate version by running the command:

.. tabbed-parsed-literal::
  :tabs: "Linux or Mac OS X",Windows
  :dependent: linux-windows
  :mapping: Linux,Windows
  :languages: console,shell-session

  $ node --version

.. _recommend-using-an-ide:

**We recommend using an IDE** when building CDAP applications, such as either `IntelliJ
<https://www.jetbrains.com/idea/>`__ or `Eclipse, <https://www.eclipse.org/>`__ as
described in the section on :ref:`development environment setup. <dev-env>`

.. _local-sandbox-setup:

.. rubric:: CDAP Sandbox Download, Installation, and Setup

There are three ways to `download <http://cask.co/downloads/#cdap>`__ and install the CDAP Sandbox:

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
