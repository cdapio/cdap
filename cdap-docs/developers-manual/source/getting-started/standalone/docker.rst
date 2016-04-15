.. meta::
    :author: Cask Data, Inc.
    :description: CDAP Docker Image
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

============
Docker Image
============

.. highlight:: console


Docker is one of the easiest ways to start working with CDAP without having to manually
configure anything. A Docker image with the CDAP SDK pre-installed is available on the Docker Hub
for download.

To use the **Docker image**, you can either use Docker's :ref:`Kitematic
<docker-kitematic>` (on Mac OS X and Windows) |---| a graphical user interface for running
Docker containers |---| or start the container from :ref:`a command line <docker-command-line>`.

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
   necessarily the most-current version:
 
     .. image:: ../../_images/kitematic/kitematic-3-select-tag.png
        :width: 4in
        :align: center

#. Close the menu by pressing the ``X`` in the circle. Press "Create" to download and start the CDAP image. 
   When it has started up, you will see in the logs a message that the CDAP UI is listening on port 9999:
 
     .. image:: ../../_images/kitematic/kitematic-4-cdap-started.png
        :width: 8in
        :align: center

#. To connect a web browser for the CDAP UI, you'll need to find the external IP addresses
   and ports that the Docker instance is exposing. The easiest way to do that is click on the
   *Settings* tab, and then the *Ports* tab:
 
     .. image:: ../../_images/kitematic/kitematic-5-links.png
        :width: 8in
        :align: center


#. This shows that the CDAP instance is listening on the internal port ``9999`` within the
   Docker instance, while the Docker instance exposes that port on the external IP address
   and port ``192.168.99.100:32769``. The text in blue is a link; clicking it will open it
   in your system web browser and connect to the CDAP UI:
   
     .. image:: ../../_images/kitematic/kitematic-6-cdap-ui.png
        :width: 8in
        :align: center


.. _docker-command-line:

Docker from a Command Line
==========================

- Docker is available for a variety of platforms. Download and install Docker in your environment by
  following the `platform-specific installation instructions <https://docs.docker.com/engine/installation/>`__
  from `Docker.com <https://docker.com>`__ to verify that Docker is working and has
  started correctly.
  
  If you are not running on Linux, you need to start the Docker Virtual Machine (VM) before you
  can use containers. For example:
  
  .. tabbed-parsed-literal::
    :tabs: "Mac OS X",Windows
    :mapping: linux,windows
    :dependent: linux-windows
    :languages: console,shell-session

    .. Mac OS X
    
    $ boot2docker start
    $ boot2docker ip
    
    .. Windows
    
    > boot2docker start
    > boot2docker ip
   
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

  .. tabbed-parsed-literal::
    :tabs: "Mac OS X",Windows
    :mapping: linux,windows
    :dependent: linux-windows
    :languages: console,shell-session

    .. Mac OS X
    
    $ docker pull caskdata/cdap-standalone:|release|
    
    .. Windows
    
    > docker pull caskdata/cdap-standalone:|release|


- Start the *Docker CDAP Virtual Machine* with:

  .. tabbed-parsed-literal::
    :tabs: "Mac OS X",Windows
    :mapping: linux,windows
    :dependent: linux-windows
    :languages: console,shell-session

    .. Mac OS X
    
    $ docker run -t -i -p 9999:9999 -p 10000:10000 caskdata/cdap-standalone:|release|
    
    .. Windows
    
    > docker run -t -i -p 9999:9999 -p 10000:10000 caskdata/cdap-standalone:|release|

- CDAP will start automatically once the CDAP Virtual Machine starts. CDAP’s Software
  Directory is under ``/opt/cdap/sdk``.

- Once CDAP starts, it will instruct you to connect to the CDAP UI with a web browser
  at ``http://localhost:9999``. Replace ``localhost`` with the Docker VM's IP address 
  (such as ``192.168.59.103``) that you obtained earlier. Start a browser and enter the
  address to access the CDAP UI.

- For a full list of Docker Commands, see the `Docker Command Line Documentation.
  <https://docs.docker.com/reference/commandline/cli/>`__

.. _docker-cdap-applications:

Docker and CDAP Applications
============================

- In order to begin building CDAP applications, have our :ref:`recommended software and tools
  <system-requirements>` installed in your environment.

.. include:: ../dev-env.rst  
   :start-line: 7

.. include:: ../start-stop-cdap.rst  
   :start-line: 4

.. include:: /_includes/building-apps.txt
