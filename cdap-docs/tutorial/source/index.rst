.. meta::
    :author: Cask Data, Inc.
    :description: Introduction to the Cask Data Application Platform
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _tutorial-intro:

===============================================================
Introduction to the Cask Data Application Platform v\ |version|
===============================================================

.. include:: ../../_common/_source/index.rst 
   :start-after: .. rubric:: Introduction to the Cask Data Application Platform
   :end-before:  These documents are your complete reference to the Cask Data Application Platform: they help


.. System Requirements and Dependencies, Download and Setup

.. include:: ../../developers-manual/source/getting-started/standalone/index.rst 
   :start-after: .. _system-requirements:
   :end-before:  Follow one of the above links for download and installation instructions.

Using Zip File 
--------------

.. include:: ../../developers-manual/source/getting-started/standalone/zip.rst
   :start-after: .. _standalone-zip-file:
   :end-before:  .. include:: ../dev-env.rst 


OLD Material

.. highlight:: console

The zip file is available on the Downloads section of the Cask Website at
http://cask.co/downloads. Once downloaded, unzip it to a directory on your machine::

  $ tar -zxvf cdap-sdk-2.5.0.zip
  
Use the cdap.sh script to start and stop the Standalone CDAP::

  $ cd cdap-sdk-2.5.0
  $ ./bin/cdap.sh start ... 
  $ ./bin/cdap.sh stop 

Or, if you are using Windows, use the batch script ``cdap.bat`` to start and stop the SDK.

Using Standalone Virtual Machine Image
--------------------------------------

- Download and install either Oracle VirtualBox or VMWare player to your environment. 
- Download the CDAP Standalone Virtual Machine (the .ova file) at http://cask.co/downloads. 
- Import the Virtual Machine into VirtualBox or VMWare Player. 
- The CDAP Standalone Virtual Machine has been configured and setup so you can be productive
  immediately: 

  - CDAP VM is configured with 4GB Default RAM (recommended).
  - The virtual machine has Ubuntu Desktop Linux installed as the operating system.
  - No password is required to enter the virtual machine; however, should you need to install
    or remove software, the admin user and password are both “cdap”.
  - 10GB of disk space is available for you to build your first CDAP project.
  - Both IntelliJ and Eclipse IDE are installed and will start when the virtual machine
    starts.
  - The CDAP SDK is installed under /Software/cdap-sdk-2.5.0.
  - The Standalone CDAP will automatically start when the virtual machine starts.
  - The Firefox web browser starts when the machine starts. Its default home page is the CDAP
    Console (http://localhost:9999). You’re welcome to install your favorite browser.
  - Maven is installed and configured to work for CDAP.
  - The Java JDK and Node JS are both installed.


Using Standalone Docker Image
-----------------------------

- Download and install Docker in your environment following the instructions at Docker.com.

- Start Docker using::

    $ boot2docker start 
  
- Pull down the CDAP Docker Image from the Docker hub using::

    $ docker pull caskdata/cdap-standalone
  
- Identify the Docker Virtual Machine’s Host Interface IP address (this address will be
  used in a later step) with::

    $ boot2docker ip
    
- Start the Docker CDAP VM with::

    $ docker run -t -i -p 9999:9999 caskdata/cdap-standalone
    
- Once you enter the Docker CDAP VM, you can start CDAP with these commands::

    $ cd cdap-sdk-2.5.0 $ ./bin/cdap.sh start 
    
- Once CDAP starts, it will instruct you to connect to the CDAP Console with a web browser
  at ``http://<host-ip>:9999``, replacing *<host-ip>* with the IP address you obtained earlier.

- Start a browser and enter the address to access the CDAP Console.

- It is recommended that you have our usually-recommended software and tools already
  installed in your environment, in order to begin building CDAP applications:

    - An IDE such as IntelliJ or Eclipse IDE
    - Apache Maven 3.0+ 
    - Java JDK 

Once CDAP is started successfully, in a web browser you will be able to see the CDAP
Console running at `localhost:9999 <http://localhost:9999>`__, where you can deploy
example applications and interact with CDAP.
