.. meta::
    :author: Cask Data, Inc.
    :description: CDAP Docker Image
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

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
       . . .

     $ docker-machine env cdap

   This will create a new Docker virtual machine using VirtualBox named ``cdap``; once
   created and running, the second command will print out the environment.

#. When you run ``docker-machine env cdap``, it will print a message on the screen such as:

   .. container:: copyable copyable-text

     ::

       export DOCKER_TLS_VERIFY="1"
       export DOCKER_HOST="tcp://192.168.99.100:2376"
       export DOCKER_CERT_PATH="/Users/<username>/.docker/machine/machines/cdap"
       export DOCKER_MACHINE_NAME="cdap"
       # Run this command to configure your shell:
       # eval $(docker-machine env cdap)

   It is essential to run these export commands (or the single ``eval`` command, ``eval
   $(docker-machine env cdap)``). Otherwise, subsequent Docker commands will fail because
   they won't be able to connect to the correct Docker VM.

#. If you are running **Docker on either Mac OS X or Microsoft Windows**, Docker is running a
   virtual Linux machine on top of your host OS. You will need to use the address shown
   above (such as ``192.168.99.100``) as the host name when either connecting to the CDAP
   UI or making an HTTP request.

#. If you are running **Docker on Microsoft Windows**, note that paths used employ Linux's
   forward-slashes (``/``) and not back-slashes (``\``).

#. Once Docker has started, pull down the *CDAP Docker Image* from the Docker Hub using:

   .. tabbed-parsed-literal::
     :tabs: "Linux or Mac OS X",Windows
     :mapping: linux,windows
     :dependent: linux-windows
     :languages: console,shell-session
     :keepslashes:

     $ docker pull caskdata/cdap-standalone:|release|

#. Start the *Standalone CDAP* Docker container with:

   .. tabbed-parsed-literal::
     :tabs: "Linux or Mac OS X",Windows
     :mapping: linux,windows
     :dependent: linux-windows
     :languages: console,shell-session
     :keepslashes:

     $ docker run -d --name cdap-sdk -p 11011:11011 -p 11015:11015 caskdata/cdap-standalone:|release|

   This will start the container (in the background), name it ``cdap-sdk``, and set the proxying of ports.

#. Start the *Standalone CDAP Docker container* with:

   .. tabbed-parsed-literal::
     :tabs: "Linux or Mac OS X",Windows
     :mapping: linux,windows
     :dependent: linux-windows
     :languages: console,shell-session
     :keepslashes:

     $ docker run -it --name cdap-sdk-debugging -p 11011:11011 -p 11015:11015 caskdata/cdap-standalone:|release| cdap sdk start --enable-debug

   This will start the container (in the foreground, the default), :ref:`enable debugging
   <debugging-standalone>`, name it ``cdap-sdk-debugging``, and set the proxying of ports.

#. CDAP will start automatically once the container starts. CDAP’s software
   directory is under ``/opt/cdap/sdk``.

#. Once CDAP starts, it will instruct you to connect to the CDAP UI with a web browser
   at :cdap-ui:`http://localhost:11011/ <>`.

#. If you are **running Docker on either Mac OS X or Microsoft Windows**, replace ``localhost``
   with the Docker VM's IP address (such as ``192.168.99.100``) that you obtained earlier.
   Start a browser and enter the address to access the CDAP UI from outside Docker.

Options Starting CDAP Containers
--------------------------------

- Starting the Standalone CDAP, in the background (default execution)

  .. tabbed-parsed-literal::
    :tabs: "Linux or Mac OS X",Windows
    :mapping: linux,windows
    :dependent: linux-windows
    :languages: console,shell-session
    :keepslashes:

    $ docker run -d --name cdap-sdk caskdata/cdap-standalone

- Use the CDAP CLI within the above *cdap-sdk* container:

  .. tabbed-parsed-literal::
    :tabs: "Linux or Mac OS X",Windows
    :mapping: linux,windows
    :dependent: linux-windows
    :languages: console,shell-session
    :keepslashes:

    $ docker exec -it cdap-sdk cdap cli

- Use the CDAP CLI in its own container (*cdap-cli*), against a remote CDAP instance at ``${CDAP_HOST}``:

  .. tabbed-parsed-literal::
    :tabs: "Linux or Mac OS X",Windows
    :mapping: linux,windows
    :dependent: linux-windows
    :languages: console,shell-session
    :keepslashes:

    $ docker run -it --name cdap-cli --rm caskdata/cdap-standalone cdap cli -u http://${CDAP_HOST}:11015

- Use the CDAP CLI in its own container (*cdap-cli*), against the above *cdap-sdk* container using container linking:

  .. tabbed-parsed-literal::
    :tabs: "Linux or Mac OS X",Windows
    :mapping: linux,windows
    :dependent: linux-windows
    :languages: console,shell-session
    :keepslashes:

    $ docker run -it --link cdap-sdk:sdk --name cdap-cli --rm caskdata/cdap-standalone sh -c 'exec cdap cli -u http://${SDK_PORT_11011_TCP_ADDR}:${SDK_PORT_11011_TCP_PORT}'

- Starting the Standalone CDAP, in the foreground, with ports forwarded:

  .. tabbed-parsed-literal::
    :tabs: "Linux or Mac OS X",Windows
    :mapping: linux,windows
    :dependent: linux-windows
    :languages: console,shell-session
    :keepslashes:

    $ docker run -it -p 11015:11015 -p 11011:11011 --name cdap-sdk caskdata/cdap-standalone cdap sdk start

- Starting the Standalone CDAP, in the foreground, with ports forwarded, and with debugging enabled:

  .. tabbed-parsed-literal::
    :tabs: "Linux or Mac OS X",Windows
    :mapping: linux,windows
    :dependent: linux-windows
    :languages: console,shell-session
    :keepslashes:

    $ docker run -it -p 11015:11015 -p 11011:11011 --name cdap-sdk caskdata/cdap-standalone cdap sdk start --enable-debug

- For information on mounting volumes and sharing data with the container, see the
  examples in Docker's documentation on `data volumes
  <https://docs.docker.com/engine/tutorials/dockervolumes/#/adding-a-data-volume>`__.
  You can either let Docker manage the storage in the container (the easiest) or you can
  `mount a host directory as a data volume
  <https://docs.docker.com/engine/tutorials/dockervolumes/#/mount-a-host-directory-as-a-data-volume>`__.
  However, if you mount a host directory, you must make sure that it exists and that
  permissions are set correctly. You pass such a directory using ``-v
  /my/own/datadir:/opt/cdap/sdk/my/own/datadir``, which mounts the ``/my/own/datadir`` from the
  host system as ``/opt/cdap/sdk/my/own/datadir`` in the container.

Controlling the CDAP Instance
-----------------------------

- To control the CDAP instance, use this command, substituting one of ``start``, ``restart``, ``stop``,
  or ``status`` for ``<command>``:

  .. tabbed-parsed-literal::
    :tabs: "Linux or Mac OS X",Windows
    :mapping: linux,windows
    :dependent: linux-windows
    :languages: console,shell-session

    $ docker exec -d cdap-sdk cdap sdk <command>

    $ docker exec -d cdap-sdk cdap sdk start
    $ docker exec -d cdap-sdk cdap sdk restart
    $ docker exec -d cdap-sdk cdap sdk stop

    # To see the status:
    $ docker exec -it cdap-sdk cdap sdk status

- When you are finished, stop CDAP and then shutdown the Docker machine:

  .. tabbed-parsed-literal::
    :tabs: "Linux or Mac OS X",Windows
    :mapping: linux,windows
    :dependent: linux-windows
    :languages: console,shell-session

    $ docker exec -d cdap-sdk cdap sdk stop
    $ docker-machine stop cdap

Docker Resources
----------------

- For a full list of Docker Commands, see the `Docker Command Line Documentation.
  <https://docs.docker.com/engine/reference/commandline/cli/>`__


.. _docker-kitematic:

Docker using Kitematic
======================

`Docker Kitematic <https://www.docker.com/docker-kitematic>`__ is available as part of the
`Docker <https://www.docker.com/products/overview>`__ for either Mac OS X or Microsoft Windows.
It is a graphical user interface for running Docker containers. Follow these steps to install
Kitematic and then download, start, and connect to a CDAP container.

#. Download and install `Docker <https://www.docker.com/products/overview>`__ for
   either Mac OS X or Microsoft Windows.

#. Download and install Kitematic for either Mac OS X or Microsoft Windows. The easiest
   method is to select *Open Kitematic* from the *Docker* menu, and follow the
   instructions for downloading and installing it:

     .. figure:: ../../_images/kitematic/kitematic-0-installing.png
        :figwidth: 100%
        :width: 300px
        :class: bordered-image

#. Start Kitematic. On Mac OS X, it will be installed in ``/Applications/Docker/Kitematic``; on
   Windows, in ``Start Menu > Docker > Kitematic``.

#. Once Kitematic has started, search for the **CDAP image** by using the search box at the
   top of the window and entering ``caskdata:cdap-standalone``. Once you have found the page,
   click on the **repository menu**, circled in red here:

     .. figure:: ../../_images/kitematic/kitematic-1-searching.png
        :figwidth: 100%
        :width: 800px
        :class: bordered-image

#. Click on the **tags** button:

     .. figure:: ../../_images/kitematic/kitematic-2-tags.png
        :figwidth: 100%
        :width: 400px
        :class: bordered-image

#. Select the **desired version**. Note that the tag *latest* is the last version that
   was put up at Docker Hub, which is not the necessarily the desired version, which is
   |bold-version| (*3.5.0* shown as an illustration):

     .. figure:: ../../_images/kitematic/kitematic-3-select-tag.png
        :figwidth: 100%
        :width: 400px
        :class: bordered-image

#. Close the menu by pressing the ``X`` in the circle. Click "Create" to download and start the CDAP image.
   When it has started up, you will see in the logs a message that the CDAP UI is listening on port 11011:

     .. figure:: ../../_images/kitematic/kitematic-4-cdap-started.png
        :figwidth: 100%
        :width: 800px
        :class: bordered-image

#. To connect a web browser for the CDAP UI, you'll need to find the external IP addresses
   and ports that the Docker host is exposing. These are listed on the right-hand side of
   the previous illustration:

     .. figure:: ../../_images/kitematic/kitematic-5-links.png
        :figwidth: 100%
        :width: 400px
        :class: bordered-image

#. This shows that the CDAP container is listening on the internal port ``11011`` within the
   Docker host, while the Docker host proxies that port on the virtual machine IP address
   and port (``localhost:32773``). Enter that address and port into your system web browser to
   connect to the CDAP UI, such as http://localhost:32773:

     .. figure:: ../../_images/kitematic/kitematic-6-cdap-ui.png
        :figwidth: 100%
        :width: 800px
        :class: bordered-image


.. _docker-cdap-applications:

Docker and CDAP Applications
============================

- In order to begin building CDAP applications, have our :ref:`recommended software and tools
  <system-requirements>` installed in your environment.

.. include:: ../dev-env.rst
   :start-line: 7

.. include:: /_includes/building-apps.txt
