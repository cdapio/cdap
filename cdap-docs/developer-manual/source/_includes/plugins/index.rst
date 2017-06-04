.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016-2017 Cask Data, Inc.

:hide-toc: true

.. _cdap-pipelines-plugins:

================
Plugin Reference
================

.. toctree::
   :maxdepth: 1

    Action Plugins <actions/index>
    Source Plugins <sources/index>
    Transform Plugins <transforms/index>
    Analytic Plugins <analytics/index>
    Sink Plugins <sinks/index>
    Shared Plugins <shared-plugins/index>
    Post-run Plugins <post-run-plugins/index>


These plugins (from CDAP Pipelines Version |cdap-pipelines-version|) are shipped with CDAP, both in the
CDAP Local Sandbox and Distributed CDAP:

- :doc:`Action Plugins <actions/index>`
- :doc:`Source Plugins <sources/index>`
- :doc:`Transform Plugins <transforms/index>`
- :doc:`Analytic Plugins <analytics/index>`
- :doc:`Sink Plugins <sinks/index>`
- :doc:`Shared Plugins <shared-plugins/index>`

  - :doc:`CoreValidator Plugin <shared-plugins/core>`

- :doc:`Post-run Plugins <post-run-plugins/index>`

.. rubric:: Plugin Notes

- Plugins are grouped here based on how they are displayed in the CDAP Studio UI.
  Each plugin is of a particular type, and all plugins that are not sources, sinks, or
  actions are grouped in *transform* plugins. Each page describing a plugin shows its type
  and version at the bottom of the page.

- The *batch sources* can write to any *batch sinks* that are available and *real-time sources*
  can write to any *real-time sinks*. *Transformations* work with either *sinks* or *sources*.
  Transformations can use *validators* to test data and check that it follows user-specified
  rules.

  Other plugin types may be restricted as to which plugin (and artifact) that they work
  with, depending on the particular functionality they provide. For instance, certain
  *model* (the *NaiveBayesTrainer*) and *compute* (the *NaiveBayesClassifier*) plugins
  (found in *analytics*) only work with batch pipelines.

- Certain plugins |---| such as the *JavaScript*, *Python Evaluator*, and *Validator*
  transforms |---| are designed to be customized by end-users with their own code, either
  from within CDAP Studio or in a configuration file. For instance, you can create your
  own data validators either by using the functions supplied in the *CoreValidator* plugin
  or by implementing and supplying your own custom validation function.

- *Action* plugins (supported only in pipelines based on the ``cdap-data-pipeline`` artifact) can
  be added to run either before a source or after a sink. A "post-run" action plugin can be
  specified that runs after the entire pipeline has run.

- Additional types of plugins are under development, and developers can create and
  add their own plugins and plugin types.

.. rubric:: Exploring Plugin Details

Details on the available plugins and the required properties for sources, analytics,
transformations (transforms), sinks, and other plugin types can be obtained and explored
using:

- :ref:`CDAP Studio <cdap-studio>`
- :ref:`CDAP CLI <cli>`
- :ref:`Artifact HTTP RESTful API <http-restful-api-artifact>`
- Individual plugin documentation for
  :doc:`actions <actions/index>`,
  :doc:`sources <sources/index>`,
  :doc:`analytics <analytics/index>`,
  :doc:`transforms <transforms/index>`,
  :doc:`sinks <sinks/index>`,
  :doc:`shared <shared-plugins/index>` and
  :doc:`post-run plugins <post-run-plugins/index>`

.. rubric:: Creating Custom Plugins

If these plugins don't meet your requirements, you can :ref:`create a custom
plugin <cdap-pipelines-developing-plugins>`.

If you are creating a custom plugin that extends the **existing system artifacts,** its
name should not collide with existing names, for ease-of-use in the CDAP UI and CDAP
Studio. You are free to create your own plugin and plugin-type, depending on the
functionality you are adding or requiring.
