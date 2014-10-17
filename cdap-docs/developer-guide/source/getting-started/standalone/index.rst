.. :author: Cask Data, Inc.
   :description: Index document
   :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Standalone Setup
============================================

.. toctree::
   :maxdepth: 1
   :titlesonly:
   
   Binary Zip File <zip>
   Virtual Machine Image <virtual-machine>
   Docker Image <docker>

There are three ways to download the CDAP SDK: 

- as a :doc:`binary zip file <zip>`;
- as a :doc:`Virtual Machine image <virtual-machine>`; or 
- as a :doc:`Docker image <docker>`.

If you already have a development environment setup, the zip file is your easiest solution.

If you don't have a development environment, the Virtual Machine offers a pre-configured
environment with CDAP pre-installed and that automatically starts applications so that you
can be productive immediately. You can build your own projects or follow the provided
example applications.

The Docker image is intended for those developing on Linux.

Starting and Stopping CDAP
--------------------------

.. highlight:: console

Use the ``cdap.sh`` script to start and stop the Standalone CDAP 
(the location will vary depending on where the CDAP SDK is installed)::

  $ cd cdap-sdk-2.5.1
  $ ./bin/cdap.sh start
  ...
  $ ./bin/cdap.sh stop

Or, if you are using Windows, use the batch script ``cdap.bat`` to start and stop the SDK.

Note that starting CDAP is not necessary if you use either the Virtual Machine or the
Docker image, as they both start the Standalone CDAP automatically on startup.

Once CDAP is started successfully, in a web browser you will be able to see the CDAP
Console running at ``localhost:9999``, where you can deploy example applications and
interact with CDAP. Note that in the case of the Docker image, you will need to substitute 
the Docker VM's IP address for ``localhost`` in the web browser address bar.