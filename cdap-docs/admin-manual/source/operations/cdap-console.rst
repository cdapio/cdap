.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _cdap-console:

==================================================================
CDAP Console
==================================================================

The **CDAP Console** is available for deploying, querying and managing the Cask Data
Application Platform in all modes of CDAP except an 
:ref:`In-memory CDAP. <in-memory-data-application-platform>`

.. image:: ../_images/console/console_01_overview.png
   :width: 600px
   :align: center

Here is a screen-capture of the CDAP Console running on a Distributed CDAP.

Down the left sidebar, underneath the **Cask** logo, are five buttons: *Application, Collect
Process, Store,* and *Query.* These buttons gives you access to CDAP Console facilities for
managing each of these CDAP components.

In the far upper-right are two buttons: the *Metrics* and *Services* buttons, which take
you to their respective explorers.

.. A detailed *How-To Guide* covering using the CDAP Console will be available
.. at `Guides and Tutorials for CDAP. <http://cask.co/guides/>`__
.. is available


New User Interface
------------------
As part of release 2.8.0, a new alpha User Interface (UI) for the CDAP Console was introduced.

To try out the new UI, changes are required before CDAP is started.

- For CDAP Standalone SDK, pass an additional argument when starting CDAP::

    $ ./bin/cdap.sh start --enable-alpha-ui
    
- For CDAP Distributed, modify the :ref:`command used to start CDAP <install-starting-services>`.
  Before starting the service, an environmental variable needs to be set::
  
    export ENABLE_ALPHA_UI=true 
    for i in `ls /etc/init.d/ | grep cdap` ; do sudo service $i restart ; done
    
  To restart just the CDAP Web App (the UI) in the new UI::
  
    export ENABLE_ALPHA_UI=true sudo /etc/init.d/cdap-web-app restart
