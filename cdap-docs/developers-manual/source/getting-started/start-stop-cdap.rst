.. :author: Cask Data, Inc.
   :copyright: Copyright © 2014-2015 Cask Data, Inc.

============================================
Starting and Stopping Standalone CDAP
============================================

.. this file is included in others; any titles need to be +

.. _start-stop-cdap:

.. highlight:: console

Use the ``cdap.sh`` script to start and stop the Standalone CDAP 
(the location will vary depending on where the CDAP SDK is installed):

.. container:: highlight

  .. parsed-literal::
  
    |$| cd cdap-sdk-|version|
    |$| ./bin/cdap.sh start
    . . .
    |$| ./bin/cdap.sh stop

Or, if you are using Windows, use the batch script ``cdap.bat`` to start and stop the SDK.

Note that starting CDAP is not necessary if you use either the Virtual Machine or the
Docker image, as they both start the Standalone CDAP automatically on startup.

Once CDAP is started successfully, in a web browser you will be able to see the CDAP
Console running at ``http://localhost:9999``, where you can deploy example applications and
interact with CDAP. 

Note that in the case of the Docker image, you will need to substitute 
the Docker VM's IP address for ``localhost`` in the web browser address bar.