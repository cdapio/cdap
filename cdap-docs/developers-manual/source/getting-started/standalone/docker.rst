.. meta::
    :author: Cask Data, Inc.
    :description: CDAP Docker Image
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

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
  
- Once Docker has started, pull down the *CDAP Docker Image* from the Docker hub using::

    $ docker pull caskdata/cdap-standalone
    
- Start the *Docker CDAP Container* with::

    $ docker run -t -i -p 9999:9999 -p 10000:10000 caskdata/cdap-standalone
    
.. include:: ../../../build/_includes/standalone-versioned.rst 
   :start-line: 12
   :end-line:   15
  
- Once CDAP starts, it will instruct you to connect to the CDAP Console with a web browser
  at ``http://<container-hostname>:9999``, such as ``http://6f0162922c37:9999``. However, we
  publish the container ports to our local machine. Start a browser and connect to
  ``http://localhost:9999`` to access the CDAP Console.

- In order to begin building CDAP applications, have our :ref:`recommended software and tools
  <system-requirements>` installed in your environment.

- For a full list of Docker Commands, see the `Docker Command Line Documentation.
  <https://docs.docker.com/reference/commandline/cli/>`__
