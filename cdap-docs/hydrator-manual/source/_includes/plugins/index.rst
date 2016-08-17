.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

:hide-toc: true

.. _cask-hydrator-plugins:

================
Plugin Reference
================

.. toctree::
   :maxdepth: 2
   
    Action Plugins <actions/index>
    Batch Plugins <batch/index>
    Real-time Plugins <realtime/index>
    Shared Plugins <shared-plugins/index>
    Post-run Plugins <post-run-plugins/index>


These plugins (from Hydrator Version |cask-hydrator-version|) are shipped with CDAP, both in the
SDK and Distributed CDAP:

- :doc:`Action Plugins <actions/index>`
- :doc:`Batch Plugins <batch/index>`

  - :doc:`Source Plugins <batch/sources/index>`
  - :doc:`Transform Plugins <batch/transforms/index>`
  - :doc:`Sink Plugins <batch/sinks/index>`

- :doc:`Real-time Plugins<realtime/index>`

  - :doc:`Source Plugins <realtime/sources/index>`
  - :doc:`Transform Plugins <realtime/transforms/index>`
  - :doc:`Sink Plugins <realtime/sinks/index>`

- :doc:`Shared Plugins <shared-plugins/index>`

  - :doc:`CoreValidator Plugin <shared-plugins/core>`

- :doc:`Post-run Plugins <post-run-plugins/index>`

.. rubric:: Plugin Notes

- Additional types of plugins are under development, and developers can create and
  add their own plugins and plugin types.
  
- Plugins are grouped here based on how they are displayed in the Hydrator Studio UI.
  Each plugin is of a particular type, and all plugins that are not sources, sinks, or
  actions are grouped in *transform* plugins. Each page describing a plugin shows it type
  and version at the bottom of the page.

- The *batch sources* can write to any *batch sinks* that are available and *real-time sources*
  can write to any *real-time sinks*. *Transformations* work with either *sinks* or *sources*.
  Transformations can use *validators* to test data and check that it follows user-specified
  rules. Other plugin types may be restricted as to which plugin (and artifact) that they
  work with, depending on the particular functionality they provide.

  For instance, certain *model* (the *NaiveBayesTrainer*) and *compute* (the
  *NaiveBayesClassifier*) plugins (found in *batch/transforms*) only work with batch pipelines.

- Certain plugins |---| such as the *JavaScript*, *Python Evaluator*, and *Validator*
  transforms |---| are designed to be customized by end-users with their own code, either
  from within Hydrator Studio or in a configuration file. For instance, you can create your
  own data validators either by using the functions supplied in the *CoreValidator* plugin
  or by implementing and supplying your own custom validation function.

- *Action* plugins (supported only in pipelines based on the ``cdap-data-pipeline`` artifact) can
  be added to run either before a source or after a sink. A "post-run" action plugin can be
  specified that runs after the entire pipeline has run.


.. rubric:: Exploring Plugin Details

Details on the available plugins and the required properties for sources, transformations
(transforms), sinks, and other plugin types can be obtained and explored using:

- :ref:`Hydrator Studio <cask-hydrator-studio>`
- :ref:`CDAP CLI <cli>`
- :ref:`Artifact HTTP RESTful API <http-restful-api-artifact>`
- Plugin detailed documentation, through the links above


.. rubric:: Creating Custom Plugins

If these plugins don't meet your requirements, you can :ref:`create a custom
plugin <cask-hydrator-developing-plugins>`.

If you are creating a custom plugin that extends the **existing system artifacts,** its
name should not collide with existing names, for ease-of-use in the CDAP UI and Cask
Hydrator Studio. You are free to create your own plugin and plugin-type, depending on the
functionality you are adding or requiring.
