.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

:hide-toc: true

.. _cask-hydrator-plugins:

================
Hydrator Plugins
================

.. toctree::
   :maxdepth: 2
   
    Batch Plugins <batch/index>
    Real-time Plugins <realtime/index>
    Shared Plugins <shared-plugins/index>
    Action and Post-action Plugins <action-post-action/index>
    Adding Third-Party Plugins <third-party>
    Creating Custom Plugins <creating>
    Installing Plugins <installing>

.. rubric:: Available Plugins

Shipped with CDAP, these plugins (Hydrator Version |cdap-hydrator-version|) are available
for creating ETL, data pipelines, and other applications.

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

- :doc:`Action and Post-action Plugins <action-post-action/index>`

  - :doc:`Action Plugins <action-post-action/actions/index>`
  - :doc:`Post-action Plugins <action-post-action/post-actions/index>`


.. rubric:: Exploring Plugin Details

Details on the available plugins and the required properties for sources, transformations
(transforms), sinks and other plugin types can be obtained  and explored using the
:ref:`Artifact HTTP RESTful API <http-restful-api-artifact>`.


.. rubric:: Third-Party Plugins: Deploying a JDBC Driver

Covers using :ref:`pre-built third-party JARs <cask-hydrator-third-party-plugins>` so that they
are accessible to other plugins and applications.


.. rubric:: Creating Custom Plugins

If these plugins don't meet your requirements, you can :ref:`create a custom
plugin <cask-hydrator-creating-custom-plugins>`.

If you are creating a custom plugin that extends the **existing system artifacts,** its name
should not collide with existing names, for ease-of-use in the CDAP UI and Cask Hydrator Studio.


.. rubric:: Installing Plugins: Packaging, Presentation, and Deployment

To **package, present,** and **deploy** your plugin, see these instructions:

- `Plugin Packaging: <installing#plugin-packaging>`__ packaging in a JAR
- `Plugin Presentation: <installing#plugin-presentation>`__ controlling how your plugin appears in the Hydrator Studio
- `Plugin Deployment: <installing#deploying-a-system-artifact>`__ deploying as either a system or user *artifact*
- `Deployment Verification: <installing#deployment-verification>`__ verifying an artifact was added successfully
