.. meta::
    :author: Cask Data, Inc.
    :description: CDAP Docker Image
    :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Docker Image
============================================

.. highlight:: console

A Docker image with CDAP pre-installed is available on the Docker Hub for download.

To use the **Docker image**:

- Docker is available for a variety of platforms. Download and install Docker in your environment by
  following the `platform-specific installation instructions <https://docs.docker.com/installation>`__
  from `Docker.com <https://docker.com>`__ to verify that Docker is working and has
  started correctly.
  
  If you are not running on Linux, you need to start the Docker Virtual Machine (VM) before you
  can use containers. For example, on MacOS, use:: 
  
    $ boot2docker start
    $ boot2docker ip
    
  to determine the Docker VM's IP address. You will need to use that address as the host
  name when either connecting to the Console or making an HTTP request.
  
  When you run ``boot2docker start``, it will print a message on the screen such as::

    To connect the Docker client to the Docker daemon, please set:
        export DOCKER_HOST=tcp://192.168.59.103:2375
        export DOCKER_CERT_PATH=/Users/.../.boot2docker/certs/boot2docker-vm
        export DOCKER_TLS_VERIFY=1

  It is essential to run these export commands (or command, if only one). Otherwise,
  subsequent Docker commands will fail because they can't tell how to connect to the
  Docker VM.

- Once Docker has started, pull down the *CDAP Docker Image* from the Docker hub using::

    $ docker pull caskdata/cdap-standalone
    
- Start the *Docker CDAP Virtual Machine* with::

    $ docker run -t -i -p 9999:9999 -p 10000:10000 caskdata/cdap-standalone
    
.. include:: ../../../build/_includes/standalone-versioned.rst 
   :start-line: 12
   :end-line:   15
  
- Once CDAP starts, it will instruct you to connect to the CDAP Console with a web browser
  at ``http://<virtual-hostname>:9999``, such as ``http://6f0162922c37:9999``. Replace
  ``<virtual-hostname>`` with the Docker VM's IP address you obtained earlier. Start a browser 
  and enter the address to access the CDAP Console.

- In order to begin building CDAP applications, have our :ref:`recommended software and tools
  <system-requirements>` installed in your environment.

- For a full list of Docker Commands, see the `Docker Command Line Documentation.
  <https://docs.docker.com/reference/commandline/cli/>`__

.. include:: ../../../build/_includes/dev-env-versioned.rst  
   :start-line: 5

.. include:: ../../../build/_includes/start-stop-cdap-versioned.rst  
   :start-line: 4

.. include:: ../building-apps.rst
   :start-line: 7
