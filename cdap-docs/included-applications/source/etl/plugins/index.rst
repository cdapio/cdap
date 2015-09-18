.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. :hide-toc: true

.. _included-apps-etl-plugins:

===========
ETL Plugins 
===========

.. rubric:: Introduction

Details of the required properties for sources, transformations (transforms), and sinks
can be explored using RESTful APIs.

If you are creating a custom plugin to extend the existing system artifacts, its name
needs to not collide with existing names.

Shipped with CDAP as part of the *cdap-etl-lib-artifact*, the plugins listed below are
available for creating ETL applications.


.. toctree::
   :maxdepth: 3
   
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
