.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. :hide-toc: true

.. _cask-hydrator-plugins:

=================
Hydrator Plugins 
=================

- Plugin types for Batch 

  - Source plugins
  - Transform plugins 
  - Aggregator 
  - Model
  - Compute
  - Sink

- Plugin types for real-time 

  - Source
  - Transform
  - Sink

- Adding third-party plugins

  - JDBC

- Creating Custom plugins
- Installing plugins	

  - UI
  - REST
  - CLI 


Details of the required properties for sources, transformations (transforms), and sinks
can be explored using RESTful APIs.

If you are creating a custom plugin to extend the existing system artifacts, its name
should not collide with existing names for ease of use in the CDAP UI.

Shipped with CDAP, the plugins listed below (Hydrator Version |cdap-hydrator-version|) are
available for creating ETL applications.


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
