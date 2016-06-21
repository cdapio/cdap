.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

:hide-toc: true

.. _cask-hydrator-plugins:

=================
Hydrator Plugins 
=================

.. toctree::
   :maxdepth: 2
   
    Plugins for Batch <batch/index>
    Plugins for Real-time <realtime/index>
    Shared Plugins <shared-plugins/index>
    Post-Action Plugins <postactions/index>
    Adding Third-Party Plugins <third-party>
    Creating Custom Plugins <creating>
    Installing Plugins <installing>


- :doc:`Plugins for Batch <batch/index>`

  - :doc:`Source Plugins <batch/sources/index>`
  - :doc:`Transform Plugins <batch/transforms/index>`
  - :doc:`Aggregator Plugins <batch/aggregators/index>`
  - :doc:`Sink Plugins <batch/sinks/index>`

..   - :doc:`Compute Plugins <batch/computes/index>`
..   - :doc:`Model Plugins <batch/models/index>`

- :doc:`Plugins for Real-time <realtime/index>`

  - :doc:`Source Plugins <realtime/sources/index>`
  - :doc:`Transform Plugins <realtime/transforms/index>`
  - :doc:`Sink Plugins <realtime/sinks/index>`

..

- :doc:`Shared Plugins <shared-plugins/index>`

  - :doc:`CoreValidator Plugin <shared-plugins/core>`

..

- :doc:`Post-Action Plugins <postactions/index>`

..

- :doc:`Adding Third-Party Plugins <third-party>`
- :doc:`Creating Custom Plugins <creating>`
- :doc:`Installing Plugins <installing>`

.. 
.. - Adding third-party plugins
.. 
..   - JDBC
.. 
.. - Creating Custom plugins
.. - Installing plugins	
.. 
..   - UI
..   - REST
..   - CLI 


Details of the required properties for sources, transformations (transforms), and sinks
can be explored using RESTful APIs.

If you are creating a custom plugin to extend the existing system artifacts, its name
should not collide with existing names for ease of use in the CDAP UI.

Shipped with CDAP, the :doc:`batch <batch/index>` and :doc:`real-time <realtime/index>`
plugins (Hydrator Version |cdap-hydrator-version|) are available for creating ETL
applications.

.. 
.. .. toctree::
..    :maxdepth: 2
..    
..     Batch Sources <batchsources/index>
..     Real-time Sources <realtimesources/index>
..     Transformations <transforms/index>
..     Batch Sinks <batchsinks/index>
..     Real-time Sinks <realtimesinks/index>
..     Shared Plugins <shared-plugins/index>
..     Third-Party Jars <third-party>

.. 
.. .. toctree::
..    :maxdepth: 2
..    
..     Batch Sources <batch/sources/index>
..     Batch Transforms <transforms/index>
..     Batch Sinks <batch/sinks/index>
..     Real-time Sources <realtime/sources/index>
..     Real-time Transforms <transforms/index>
..     Real-time Sinks <realtime/sinks/index>

..     Shared Plugins <shared-plugins/index>
..     Third-Party Jars <third-party>
 
.. rubric:: Exploring Plugin Details

Details on the available plugins can be obtained using the
:ref:`Artifacts HTTP RESTful API <http-restful-api-artifact>`.
