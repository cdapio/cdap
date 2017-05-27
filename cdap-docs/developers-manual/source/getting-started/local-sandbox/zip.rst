.. meta::
    :author: Cask Data, Inc.
    :description: CDAP SDK Zip
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

.. highlight:: console

===============
Binary Zip File
===============

.. _local-sandbox-zip-file:

The **zip file** is available on the Downloads section of the Cask Website at
`<http://cask.co/downloads/#cdap>`__. Click the tab marked "Local Sandbox" for the *CDAP
Local Sandbox*. There will be a button to download the latest version.

The CDAP Local Sandbox includes the software required for development and a version of
CDAP suitable for running on a laptop.

Once downloaded, unzip it to a directory on your machine:

.. tabbed-parsed-literal::

  .. Linux

  $ unzip cdap-local-sandbox-|release|.zip

  .. Windows

  > jar xf cdap-local-sandbox-|release|.zip


.. include:: index.rst
  :start-after: .. _system-requirements:
  :end-before: .. _recommend-using-an-ide:

.. include:: ../dev-env.rst
   :start-line: 7

.. include:: ../start-stop-cdap.rst
   :start-line: 4
   :end-line:   26

.. include:: ../start-stop-cdap.rst
   :start-line: 30
   :end-line:   33

.. include:: /_includes/building-apps.txt
