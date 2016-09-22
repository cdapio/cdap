.. :author: Cask Data, Inc.
   :copyright: Copyright © 2014-2015 Cask Data, Inc.

============================================
Starting and Stopping Standalone CDAP
============================================

.. this file is included in others; any titles need to be +

.. _start-stop-cdap:

.. highlight:: console

Use the ``cdap sdk`` script (or, if you are using Windows, use ``cdap.bat sdk``) to start and
stop the Standalone CDAP (the location will vary depending on where the CDAP SDK is
installed):

.. tabbed-parsed-literal::
  
    $ cd cdap-sdk-|version|
    $ ./bin/cdap sdk start
    . . .
    $ ./bin/cdap sdk stop


.. include:: /_includes/windows-note.txt

Note that starting CDAP is not necessary if you use either the Virtual Machine or the
Docker image, as they both start the Standalone CDAP automatically on startup.

Once CDAP is started successfully, in a web browser you will be able to see the CDAP
UI running at ``http://localhost:11011``, where you can deploy example applications and
interact with CDAP. 

Note that in the case of the Docker container running inside a Virtual Machine (as on
either Mac OS X or Microsoft Windows), you will need to substitute the Docker VM's IP
address for ``localhost`` in the web browser address bar.
