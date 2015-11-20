.. meta::
    :author: Cask Data, Inc.
    :description: CDAP Docker Image
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

============
Docker Image
============

.. highlight:: console

A Docker image with CDAP SDK pre-installed is available on the Docker Hub for download.

To use the **Docker image**, you can either use Docker's :ref:`Kitematic
<docker-kitematic>` (on Mac OS X and Windows), a graphical user interface for running
Docker containers, or start the container from :ref:`a command line <docker-command-line>`.

.. _docker-kitematic:

Docker using Kitematic
======================

`Docker Kitematic <https://www.docker.com/docker-kitematic>`__ is available as part of the
`Docker Toolbox <https://www.docker.com/docker-toolbox>`__ for either Mac OS X or Microsoft Windows.
It is a graphical user interface for running Docker containers. Follow these steps to install 
Kitematic and then download, start, and connect to an instance of CDAP.

#. Download and install the `Docker Toolbox <https://www.docker.com/docker-toolbox>`__ for 
   either Mac OS X or Microsoft Windows.

#. Start Kitematic. On Mac OS X, it will be installed in ``/Applications/Docker/Kitematic``; on 
   Windows, in ``??``.
   
#. Once Kitematic has started (you can either login to the Docker Hub or skip that step), search for 
the CDAP image using the search box at the top of the window:

   .. image:: ../_images/kitematic-1-start.png
      :width: 5in
      :align: center




.. _docker-command-line:

Docker from a Command Line
==========================

- Docker is available for a variety of platforms. Download and install Docker in your environment by
  following the `platform-specific installation instructions <https://docs.docker.com/installation>`__
  from `Docker.com <https://docker.com>`__ to verify that Docker is working and has
  started correctly.
  
  If you are not running on Linux, you need to start the Docker Virtual Machine (VM) before you
  can use containers. For example, on Mac OS, use::
  
    $ boot2docker start
    $ boot2docker ip
    
  to determine the Docker VM's IP address. You will need to use that address as the host
  name when either connecting to the CDAP UI or making an HTTP request.
  
  When you run ``boot2docker start``, it will print a message on the screen such as::

    To connect the Docker client to the Docker daemon, please set:
        export DOCKER_HOST=tcp://192.168.59.103:2375
        export DOCKER_CERT_PATH=/Users/.../.boot2docker/certs/boot2docker-vm
        export DOCKER_TLS_VERIFY=1

  It is essential to run these export commands (or command, if only one). Otherwise,
  subsequent Docker commands will fail because they can't tell how to connect to the
  Docker VM.

- Once Docker has started, pull down the *CDAP Docker Image* from the Docker hub using:

  .. container:: highlight

    .. parsed-literal::
  
      |$| docker pull caskdata/cdap-standalone:|release|

- Start the *Docker CDAP Virtual Machine* with:

  .. container:: highlight

    .. parsed-literal::
  
      |$| docker run -t -i -p 9999:9999 -p 10000:10000 caskdata/cdap-standalone:|release|

- CDAP will start automatically once the CDAP Virtual Machine starts. CDAP’s Software
  Directory is under ``/opt/cdap/sdk``.

- Once CDAP starts, it will instruct you to connect to the CDAP UI with a web browser
  at ``http://localhost:9999``. Replace ``localhost`` with the Docker VM's IP address 
  (such as ``192.168.59.103``) that you obtained earlier. Start a browser and enter the
  address to access the CDAP UI.

- In order to begin building CDAP applications, have our :ref:`recommended software and tools
  <system-requirements>` installed in your environment.

- For a full list of Docker Commands, see the `Docker Command Line Documentation.
  <https://docs.docker.com/reference/commandline/cli/>`__

.. include:: ../dev-env.rst  
   :start-line: 7

.. include:: ../start-stop-cdap.rst  
   :start-line: 4

.. include:: building-apps.txt
