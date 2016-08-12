.. meta::
    :author: Cask Data, Inc.
    :description: CDAP Virtual Machine
    :copyright: Copyright © 2014-2016 Cask Data, Inc.

============================================
Virtual Machine Image
============================================

.. index:: VM, Virtual, Machine

.. highlight:: console

To use the **Virtual Machine image**:

- Download and install either `Oracle VirtualBox <https://www.virtualbox.org>`__ or
  `VMWare <http://www.vmware.com/products/player>`__ player to your environment.
- Download the CDAP Standalone Virtual Machine (*Standalone VM*) at `<http://cask.co/downloads/#cdap>`__.
- Import the downloaded ``.ova`` file into either the VirtualBox or VMWare Player.

The CDAP Standalone Virtual Machine is configured with the recommended settings for Standalone CDAP:

- 4 GB of RAM
- Ubuntu Desktop Linux
- 40 GB of virtual disk space

It has pre-installed all the software that you need to run and develop CDAP applications:
   
- The Standalone CDAP SDK is installed under ``/opt/cdap/sdk``
  and will automatically start when the virtual machine starts.
- A Java JDK is installed.
- Maven is installed and configured to work for CDAP.
- Both IntelliJ IDEA and Eclipse IDE are installed and available through desktop links once the
  virtual machine has started.
- Links on the desktop are provided to the CDAP SDK, CDAP UI, and CDAP documentation.
- The Chromium web browser is included. The default page for the CDAP UI, available through a desktop link, is
  ``http://localhost:11011``.

No password is required to enter the virtual machine; however, should you need to install or
remove software, the admin user and password are both ``cdap``.

.. include:: ../dev-env.rst  
   :start-line: 7
   :end-line: 23

.. tabbed-parsed-literal::
   :tabs: "Linux Virtual Machine"
   :independent:

   $ mvn archetype:generate \
       -DarchetypeGroupId=co.cask.cdap \
       -DarchetypeArtifactId=cdap-app-archetype \
       -DarchetypeVersion=\ |release|

.. include:: ../dev-env.rst  
   :start-line: 30

Starting and Stopping Standalone CDAP
============================================

Use the ``cdap.sh`` script to start and stop the Standalone CDAP:

.. tabbed-parsed-literal::
   :tabs: "Linux Virtual Machine"
   :independent:

   $ /opt/cdap/sdk/bin/cdap.sh start
     . . .
   $ /opt/cdap/sdk/bin/cdap.sh stop

Note that starting CDAP is not necessary if you use the Virtual Machine, as it
starts the Standalone CDAP automatically on startup.

Once CDAP is started successfully, in a web browser you will be able to see the CDAP
UI running at ``http://localhost:11011``, where you can deploy example applications and
interact with CDAP. 

.. include:: /_includes/building-apps.txt
