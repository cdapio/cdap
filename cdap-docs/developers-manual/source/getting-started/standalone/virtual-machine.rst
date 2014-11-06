.. meta::
    :author: Cask Data, Inc.
    :description: CDAP Virtual Machine
    :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Virtual Machine Image
============================================

.. highlight:: console

To use the **Virtual Machine image**:

- Download and install either `Oracle VirtualBox <https://www.virtualbox.org>`__ or
  `VMWare <http://www.vmware.com/products/player>`__ player to your environment.
- Download the CDAP Standalone Virtual Machine (*Standalone VM*) at `<http://cask.co/downloads/#cdap>`__.
- Import the downloaded ``.ova`` file into either the VirtualBox or VMWare Player.

The CDAP Standalone Virtual Machine is configured with the recommended settings for Standalone CDAP:

- 4 GB of RAM
- Ubuntu Desktop Linux
- 10 GB of disk space

It has pre-installed all the software that you need to run and develop CDAP applications:

.. include:: ../../../build/_includes/standalone-versioned.rst 
   :start-line: 2
   :end-line:   10

No password is required to enter the virtual machine; however, should you need to install or
remove software, the admin user and password are both ``cdap``.

.. include:: ../../../build/_includes/dev-env-versioned.rst  
   :start-line: 5

.. include:: ../../../build/_includes/start-stop-cdap-versioned.rst  
   :start-line: 4
   :end-line:   29

.. include:: ../building-apps.rst
   :start-line: 7
