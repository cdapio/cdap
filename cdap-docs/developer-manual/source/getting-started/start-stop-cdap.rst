.. meta::
    :author: Cask Data, Inc.
    :description: Starting and Stopping CDAP Sandbox
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

========================================
Starting and Stopping CDAP Sandbox
========================================

.. this file is included in others; any titles need to be +

.. _start-stop-cdap:

.. highlight:: console

Use the ``cdap sandbox`` script (or, if you are using Windows, use ``cdap.bat sandbox``)
to start and stop the CDAP Sandbox (the location will vary depending on where the
CDAP Sandbox is installed):

.. tabbed-parsed-literal::

    $ cd cdap-sandbox-|version|
    $ ./bin/cdap sandbox start
    . . .
    $ ./bin/cdap sandbox stop


You can configure CDAP by editing the ``cdap-site.xml`` file under your ``conf`` directory.
CDAP must be restarted in order for changes in configuration to be picked up. 

To run Spark2 programs with the CDAP Sandbox, edit the ``app.program.spark.compat`` setting
in your ``cdap-site.xml`` file to be ``spark2_2.11``. When the CDAP Sandbox is using
Spark2, Spark1 programs cannot be run. When the CDAP Sandbox is using Spark1, Spark2 programs
cannot be run.

.. include:: /_includes/windows-note.txt

Note that starting CDAP is not necessary if you use either the Virtual Machine or the
Docker image, as they both start the CDAP Sandbox automatically on startup.

Once CDAP is started successfully, in a web browser you will be able to see the CDAP UI
running at :cdap-ui:`http://localhost:11011/ <>`, where you can deploy example
applications and interact with CDAP.

Note that in the case of the Docker container running inside a Virtual Machine (as on
either Mac OS X or Microsoft Windows), you will need to substitute the Docker VM's IP
address for ``localhost`` in the web browser address bar.
