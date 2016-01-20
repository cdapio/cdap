.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. :hide-toc: true

.. _cdap-apps-etl-plugins:

===========
ETL Plugins 
===========

Details of the required properties for sources, transformations (transforms), and sinks
can be explored using RESTful APIs.

If you are creating a custom plugin to extend the existing system artifacts, its name
should not collide with existing names for ease of use in the CDAP UI.

Shipped with CDAP, the plugins listed below are available for creating ETL applications.


.. toctree::
   :maxdepth: 2
   
    Batch Sources <batchsources/index>
    Real-time Sources <realtimesources/index>
    Transformations <transforms/index>
    Batch Sinks <batchsinks/index>
    Real-time Sinks <realtimesinks/index>
    Shared Plugins <shared-plugins/index>
    Third-Party Jars <third-party>

  
.. rubric:: Exploring Plugin Details

Details on the available plugins can be obtained using the
:ref:`Artifacts HTTP RESTful API <http-restful-api-artifact>`.
