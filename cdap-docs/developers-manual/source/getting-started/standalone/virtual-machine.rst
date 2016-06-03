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
   
- Java JDK 7 or 8 and Node.js are both installed.
- Maven is installed and configured to work for CDAP.
- The Standalone CDAP SDK is installed under ``/opt/cdap/sdk``
  and will automatically start when the virtual machine starts.
- Both IntelliJ and Eclipse IDE are installed and available through desktop links once the
  virtual machine has started.
- Links on the desktop are provided to the CDAP SDK and CDAP UI.
- The Chromium web browser starts when the machine starts. Its default home page is the CDAP UI,
  ``http://localhost:9999``.

No password is required to enter the virtual machine; however, should you need to install or
remove software, the admin user and password are both ``cdap``.


.. include:: ../dev-env.rst  
   :start-line: 7
   :end-line: 23

.. tabbed-parsed-literal::
   :tabs: Linux

    $ mvn archetype:generate \
        -DarchetypeGroupId=co.cask.cdap \
        -DarchetypeArtifactId=cdap-app-archetype \
        -DarchetypeVersion=\ |release|


.. tabbed-parsed-literal::
   :tabs: Linux
   :independent:

  $ mvn archetype:generate \
      -DarchetypeGroupId=co.cask.cdap \
      -DarchetypeArtifactId=cdap-app-archetype \
      -DarchetypeVersion=\ |release|



.. highlight:: console

.. container:: highlight

  .. parsed-literal::
  
    |$| mvn archetype:generate \\
        -DarchetypeGroupId=co.cask.cdap \\
        -DarchetypeArtifactId=cdap-app-archetype \\
        -DarchetypeVersion=\ |release|

.. include:: ../dev-env.rst  
   :start-line: 30

Starting and Stopping Standalone CDAP
============================================

.. highlight:: console

Use the ``cdap.sh`` script to start and stop the Standalone CDAP:

.. container:: highlight

  .. parsed-literal::
  
    |$| cd /opt/cdap/sdk
    |$| ./bin/cdap.sh start
    . . .
    |$| ./bin/cdap.sh stop


Note that starting CDAP is not necessary if you use the Virtual Machine, as it
starts the Standalone CDAP automatically on startup.

Once CDAP is started successfully, in a web browser you will be able to see the CDAP
UI running at ``http://localhost:9999``, where you can deploy example applications and
interact with CDAP. 

.. include:: /_includes/building-apps.txt
