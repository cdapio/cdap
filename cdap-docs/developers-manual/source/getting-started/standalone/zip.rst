.. meta::
    :author: Cask Data, Inc.
    :description: CDAP SDK Zip
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

.. highlight:: console
  
===============
Binary Zip File
===============

.. _standalone-zip-file:

The **zip file** is available on the Downloads section of the Cask Website at
`<http://cask.co/downloads/#cdap>`__. Click the tab marked "SDK" for the *Software
Development Kit (SDK).* There will be a button to download the latest version.

The SDK includes the software required for development and a Standalone version of CDAP,
suitable for running on a laptop.

Once downloaded, unzip it to a directory on your machine:

.. tabbed-parsed-literal::

  .. Linux

  $ unzip cdap-sdk-|release|.zip
  
  .. Windows

  > jar xf cdap-sdk-|release|.zip


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
