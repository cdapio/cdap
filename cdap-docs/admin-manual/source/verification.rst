.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

.. _admin-manual-verification:

============
Verification
============

To verify that the CDAP software is successfully installed and you are able to use your
Hadoop cluster, run an example application. We provide, in our CDAP Sandbox,
pre-built JAR files for convenience.

#. Download and install the latest `CDAP Sandbox <http://cask.co/downloads/#cdap>`__.
#. Extract to a folder (``CDAP_HOME``).
#. Open a command prompt and navigate to ``CDAP_HOME/examples``.
#. Each example folder has a ``.jar`` file in its ``target`` directory.
   For verification, we will use the :ref:`WordCount example <examples-word-count>`.
#. Open a web browser to the CDAP UI.
   It is located on port ``11011`` of the box where you installed the CDAP UI service.
#. On the UI, click the button *Add App*.
#. Find the pre-built |literal-Wordcount-release-jar| using the dialog box to navigate to
   ``CDAP_HOME/examples/WordCount/target/``.
#. Once the application is deployed, instructions on running the example can be found at the
   :ref:`WordCount example <examples-word-count>`.
#. You should be able to start the application, inject sentences, and retrieve results.
#. When finished, you can stop and remove the application as described in the section on
   :ref:`cdap-building-running`.


.. _admin-manual-health-check:

.. rubric:: Getting a Health Check

.. include:: operations/index.rst
   :start-after: .. _operations-health-check:
