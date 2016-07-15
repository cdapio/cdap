.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

:hide-toc: true

.. _cask-hydrator-plugins:

=================
Plugins Reference
=================

.. toctree::
   :maxdepth: 2
   
    Batch Plugins <batch/index>
    Real-time Plugins <realtime/index>
    Shared Plugins <shared-plugins/index>
    Action Plugins <actions/index>
    Pre- and Post-run Plugins <pre-post-run/index>

.. rubric:: Available Plugins

These plugins (from Hydrator Version |cdap-hydrator-version|) are shipped with CDAP, both in the
SDK and Distributed CDAP:

- :doc:`Batch Plugins <batch/index>`

  - :doc:`Source Plugins <batch/sources/index>`
  - :doc:`Transform Plugins <batch/transforms/index>`
  - :doc:`Aggregator Plugins <batch/aggregators/index>`
  - :doc:`Compute Plugins <batch/computes/index>`
  - :doc:`Model Plugins <batch/models/index>`
  - :doc:`Sink Plugins <batch/sinks/index>`

- :doc:`Real-time Plugins<realtime/index>`

  - :doc:`Source Plugins <realtime/sources/index>`
  - :doc:`Transform Plugins <realtime/transforms/index>`
  - :doc:`Sink Plugins <realtime/sinks/index>`

- :doc:`Shared Plugins <shared-plugins/index>`

  - :doc:`CoreValidator Plugin <shared-plugins/core>`

- :doc:`Action Plugins <actions/index>`
- :doc:`Post-run Plugins <pre-post-run/post-run-plugins/index>`


.. rubric:: Exploring Plugin Details

Details on the available plugins and the required properties for sources, transformations
(transforms), sinks and other plugin types can be obtained  and explored using the
:ref:`Artifact HTTP RESTful API <http-restful-api-artifact>`.

.. rubric:: Creating Custom Plugins

If these plugins don't meet your requirements, you can :ref:`create a custom
plugin <cask-hydrator-developing-plugins>`.

If you are creating a custom plugin that extends the **existing system artifacts,** its name
should not collide with existing names, for ease-of-use in the CDAP UI and Cask Hydrator Studio.
