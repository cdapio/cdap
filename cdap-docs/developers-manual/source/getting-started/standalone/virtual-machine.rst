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
   
- Java JDK 7 and Node.js are both installed.
- Maven is installed and configured to work for CDAP.
- The Standalone CDAP SDK is installed under ``/Software/cdap-sdk-``\ |literal-release|
  and will automatically start when the virtual machine starts.
- Both IntelliJ and Eclipse IDE are installed and will start when the virtual machine starts.
- The Firefox web browser starts when the machine starts. Its default home page is the CDAP Console,
  ``http://localhost:9999``.

No password is required to enter the virtual machine; however, should you need to install or
remove software, the admin user and password are both ``cdap``.

.. include:: ../dev-env.rst  
   :start-line: 7

.. include:: ../start-stop-cdap.rst  
   :start-line: 4
   :end-line:   33

.. include:: ../building-apps.rst
   :start-line: 7
