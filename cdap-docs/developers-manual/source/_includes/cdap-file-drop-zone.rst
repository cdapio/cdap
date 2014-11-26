CDAP File DropZone
==================

File DropZone allows users to easily perform the bulk ingestion of local
files. Files can either be directly uploaded, or they can be copied to a
*work\_dir*, where they will automatically be ingested by a daemon
process.

Features
--------

-  Distributed as debian and rpm packages;
-  Loads properties from a configuration file;
-  Supports multiple observers/topics;
-  Able to survive restart and resumes sending from the first unsent
   record of each of the existing files; and
-  Removes files that are completely sent.

Usage
-----

To install File DropZone, execute one of these commands:

-  on Debian/Ubuntu systems:

   ::

       sudo apt-get install cdap-file-drop-zone-1.0.2.deb

-  on RHEL/CentOS systems:

   ::

       sudo rpm -ivh --force cdap-file-drop-zone-1.0.2.rpm

Repository information can be found in the `Distributed CDAP
Installation
instructions <http://docs.cask.co/cdap/current/en/admin-manual/installation/installation.html#packaging>`__.

Once installed, configure the daemon by editing the file:

::

    /etc/file-drop-zone/conf/file-drop-zone.properties

These parameters must be specified:

::

    polling_interval
    observers
    work_dir
    observers.obs1.pipe
    pipes.pipe1.sink.stream_name
    pipes.pipe1.sink.host
    pipes.pipe1.sink.port

To start the daemon, execute the command:

::

    sudo service file-drop-zone start

To stop the daemon, execute the command:

::

    sudo service file-drop-zone stop

File DropZone stores log files in the ``/var/log/file-drop-zone``
directory. PID, states and statistics are stored in the
``/var/run/file-drop-zone`` directory

Uploading A File Directly
-------------------------

To upload a file that is outside of the *work\_dir*, execute the
command:

::

    file-drop-zone load <file-path> <observer>

If only one observer is configured, the *observer* parameter is not
required:

::

    file-drop-zone load <file-path>

Authentication Client
---------------------

Once File DropZone is installed, configure the Authentication Client by
editing the properties file:

::

    /etc/file-drop-zone/conf/auth-client.properties

Authentication Client configuration parameters:

-  ``pipes.<pipe-name>.sink.auth_client`` - classpath of authentication
   client class
-  ``pipes.<pipe-name>.sink.auth_client_properties`` - path to
   authentication client properties file

Authentication Client Example Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    # User name
    security.auth.client.username=admin
    # User password
    security.auth.client.password=realtime

Example Configuration
---------------------

This configuration file will set the File DropZone application to
observe 2 directories using 2 pipes:

::

    # Polling directories interval in milliseconds
    polling_interval=5000

    # Comma-separated list of directories observers to be configured
    observers=obs1

    #Path to work directory
    work_dir=/var/file-drop-zone/

    # General observer configurations
    # Pipe is used for loading data from the file to the Stream
    observers.obs1.pipe=pipe1

    # Pipe sink properties
    # Name of the stream
    pipes.pipe1.sink.stream_name=logEventStream
    # Host name that is used by stream client
    pipes.pipe1.sink.host=localhost
    # Host port that is used by stream client
    pipes.pipe1.sink.port=10000
    # Whether or not the sink endpoint uses SSL
    pipes.pipe1.sink.ssl=true
    # Whether or not the certificate should be checked if using SSL
    pipes.pipe1.sink.verify.ssl.cert=true
    # Path to authentication client properties to use if SSL is being used
    pipes.pipe1.sink.auth_client_properties=/etc/file-drop-zone/conf/auth-client.properties

