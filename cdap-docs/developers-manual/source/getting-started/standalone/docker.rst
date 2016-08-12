.. meta::
    :author: Cask Data, Inc.
    :description: CDAP Docker Image
    :copyright: Copyright © 2014-2016 Cask Data, Inc.

============
Docker Image
============

.. highlight:: console


Docker is one of the easiest ways to start working with CDAP without having to manually
configure anything. A Docker image with the CDAP SDK pre-installed is available on the Docker Hub
for download.

To use the **Docker image**, you can either start the container from :ref:`a command line
<docker-command-line>` or use Docker's :ref:`Kitematic <docker-kitematic>` (on Mac OS X
and Windows), a graphical user interface for running Docker containers.

.. _docker-command-line:

Docker from a Command Line
==========================

Docker environments are available for a variety of platforms. Download and install Docker for your platform by
following the `platform-specific installation instructions <https://docs.docker.com/engine/installation/>`__
from `Docker.com <https://docker.com>`__, and verify that the Docker environment is working and has
started correctly.
  
#. If you are not running on Linux, you will need to create and start a Docker Virtual Machine (VM) before you
   can use containers. For example:
   
   .. tabbed-parsed-literal::
     :tabs: "Mac OS X",Windows
     :mapping: linux,windows
     :dependent: linux-windows
     :languages: console,shell-session
     
     $ docker-machine create --driver virtualbox cdap
     $ docker-machine env cdap
     
   This will create a new Docker virtual machine using VirtualBox named ``cdap`` and print out the environment.

#. When you run ``docker-machine env cdap``, it will print a message on the screen such as::

     export DOCKER_TLS_VERIFY="1"
     export DOCKER_HOST="tcp://192.168.99.100:2376"
     export DOCKER_CERT_PATH="/Users/<username>/.docker/machine/machines/cdap"
     export DOCKER_MACHINE_NAME="cdap"
     # Run this command to configure your shell: 
     # eval $(docker-machine env cdap)
 
   It is essential to run these export commands (or the single ``eval`` command). Otherwise,
   subsequent Docker commands will fail because they won't be able to connect to the correct
   Docker VM.
  
#. If you are running **Docker on either Mac OS X or Microsoft Windows**, Docker is running a
   virtual Linux machine on top of your host OS. You will need to use the address shown
   above (such as ``192.168.99.100``) as the host name when either connecting to the CDAP
   UI or making an HTTP request.

#. Once Docker has started, pull down the *CDAP Docker Image* from the Docker Hub using:

   .. tabbed-parsed-literal::
     :tabs: "Linux or Mac OS X",Windows
     :mapping: linux,windows
     :dependent: linux-windows
     :languages: console,shell-session
 
     .. Linux or Mac OS X
     
     $ docker pull caskdata/cdap-standalone:|release|

     .. Windows
    
     > docker pull caskdata/cdap-standalone:|release|

#. Start the *CDAP Standalone Docker container* with:

   .. tabbed-parsed-literal::
     :tabs: "Linux or Mac OS X",Windows
     :mapping: linux,windows
     :dependent: linux-windows
     :languages: console,shell-session
 
     $ docker run -d --name cdap-standalone -p 11011:11011 -p 11015:11015 caskdata/cdap-standalone:|release|
     
   This will start the container, name it ``cdap-standalone``, and setup the proxying of ports.

#. CDAP will start automatically once the container starts. CDAP’s software
   directory is under ``/opt/cdap/sdk``.

#. Once CDAP starts, it will instruct you to connect to the CDAP UI with a web browser
   at ``http://localhost:11011``. 
  
#. If you are **running Docker on either Mac OS X or Microsoft Windows**, replace ``localhost`` 
   with the Docker VM's IP address (such as ``192.168.99.100``) that you obtained earlier.
   Start a browser and enter the address to access the CDAP UI from outside Docker.

#. To control the CDAP instance, use this command, substituting one of ``start``, ``restart``, ``status``,
   or ``stop`` for ``<command>``:

   .. tabbed-parsed-literal::
     :tabs: "Linux or Mac OS X",Windows
     :mapping: linux,windows
     :dependent: linux-windows
     :languages: console,shell-session
     
     .. Linux or Mac OS X
     
     $ docker exec -d cdap-standalone /opt/cdap/sdk/bin/cdap.sh <command>

     .. Windows
     
     > docker exec -d cdap-standalone /opt/cdap/sdk/bin/cdap.sh <command>

#. When you are finished, stop CDAP and then shutdown Docker:

   .. tabbed-parsed-literal::
     :tabs: "Linux or Mac OS X",Windows
     :mapping: linux,windows
     :dependent: linux-windows
     :languages: console,shell-session
     
     .. Linux or Mac OS X
     
     $ docker exec -d cdap-standalone /opt/cdap/sdk/bin/cdap.sh stop
     $ docker-machine stop cdap

     .. Windows
     
     > docker exec -d cdap-standalone /opt/cdap/sdk/bin/cdap.sh stop
     > docker-machine stop cdap

#. For a full list of Docker Commands, see the `Docker Command Line Documentation.
   <https://docs.docker.com/reference/commandline/cli/>`__


.. _docker-kitematic:

Docker using Kitematic
======================

`Docker Kitematic <https://www.docker.com/docker-kitematic>`__ is available as part of the
`Docker Toolbox <https://www.docker.com/docker-toolbox>`__ for either Mac OS X or Microsoft Windows.
It is a graphical user interface for running Docker containers. Follow these steps to install 
Kitematic and then download, start, and connect to a CDAP container.

#. Download and install the `Docker Toolbox <https://www.docker.com/docker-toolbox>`__ for 
   either Mac OS X or Microsoft Windows.

#. Start Kitematic. On Mac OS X, it will be installed in ``/Applications/Docker/Kitematic``; on 
   Windows, in ``Start Menu > Docker > Kitematic``.
   
#. Once Kitematic has started, search for the **CDAP image** using the search box at the
   top of the window. Then click on the repository menu, circled in red here:
 
     .. image:: ../../_images/kitematic/kitematic-1-searching.png
        :width: 8in
        :align: center

#. Click on the tags button:
 
     .. image:: ../../_images/kitematic/kitematic-2-tags.png
        :width: 4in
        :align: center

#. Select the desired version.
   Note that the tag **latest** is the last version that was put up at Docker Hub, which is not the 
   necessarily the desired version, which is |literal-version|:
 
     .. image:: ../../_images/kitematic/kitematic-3-select-tag.png
        :width: 4in
        :align: center

#. Close the menu by pressing the ``X`` in the circle. Press "Create" to download and start the CDAP image. 
   When it has started up, you will see in the logs a message that the CDAP UI is listening on port 11011:
 
     .. image:: ../../_images/kitematic/kitematic-4-cdap-started.png
        :width: 8in
        :align: center

#. To connect a web browser for the CDAP UI, you'll need to find the external IP addresses
   and ports that the Docker host is exposing. The easiest way to do that is click on the
   *Settings* tab, and then the *Ports* tab:
 
     .. image:: ../../_images/kitematic/kitematic-5-links.png
        :width: 8in
        :align: center

#. This shows that the CDAP container is listening on the internal port ``11011`` within the
   Docker host, while the Docker host proxies that port on the virtual machine IP address
   and port (``192.168.99.100:32769``). Enter that address and port into your system web browser to
   connect to the CDAP UI:
   
     .. image:: ../../_images/kitematic/kitematic-6-cdap-ui.png
        :width: 8in
        :align: center


.. _docker-cdap-applications:

Docker and CDAP Applications
============================

- In order to begin building CDAP applications, have our :ref:`recommended software and tools
  <system-requirements>` installed in your environment.

.. include:: ../dev-env.rst  
   :start-line: 7

.. include:: /_includes/building-apps.txt
