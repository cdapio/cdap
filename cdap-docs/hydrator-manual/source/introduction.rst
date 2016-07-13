.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _cask-hydrator-introduction:

========================
Introduction to Hydrator
========================

What is Cask Hydrator?
======================
Cask Hydrator (*Hydrator*) is a self-service, reconfigurable, extendable framework to
develop, run, automate, and operate data pipelines on Hadoop. Completely open source, it
is licensed under the Apache 2.0 license.

Hydrator is an extension to CDAP and includes the *Cask Hydrator Studio*, a visual
drag-and-drop interface for building data pipelines from an included library of pre-built
plugins.

It provides an operational view of the resulting pipeline that allows for lifecycle
control and monitoring of the metrics, logs, and other runtime information.

Though the typical uses of pipelines vary from ETL (extract-transform-load) of data to
sentiment analysis through to the preparation of daily aggregations and reports, Hydrator
can be adapted to an increasing number of situations and requirements.

What is a Pipeline?
===================
Pipelines are applications |---| specifically for the processing of data flows |---|
created from artifacts. The data flows can be either batch or real-time, and a variety of
processing paradigms (MapReduce, Spark, etc.) can be used.

A pipeline can be thought of as consisting of a series of *stages*. Each stage is a usage
of a *plugin*, an extension to CDAP that provides a specific functionality.

A stage's configuration properties describes what that plugin is to do (read from a
stream, write to a table, run a script), and is dependent on the particular plugin used.

All stages are connected together in a directed acyclic graph (or DAG), which is
represented in *Hydrator Studio* as a connected series of icons:

.. image:: /_images/forkInPipeline.png
   :width: 6in
   :align: center

After the pipeline has been run, "post-action" plugins can be called, to perform actions
such as emailing notifications, moving files, and running database queries, even if the
pipeline run succeeded or failed.

Types of Pipelines
------------------
Batch applications can be scheduled to run periodically using a cron expression and can
read data from batch sources using a MapReduce job. The batch application then performs
any of a number of optional transformations before writing to one or more batch sinks.

Real-time applications are designed to poll sources periodically to fetch the data,
perform any optional transformations required, and then write to one or more real-time
sinks.

Creating Pipelines
------------------
Pipelines are created from artifacts. A number of artifacts are supplied with CDAP, and
custom artifacts can be created by developers. An artifact is a blueprint or template
that |---| with the addition of a configuration file |---| is used to create an application.

A pipeline application is created by preparing a configuration that specifies the artifact
and which source, transformations (also known as transforms), and sinks are used to create
the application. The configuration can either be written as a JSON file or, in the case of
the CDAP UI, specified in-memory.

CDAP currently provides three artifacts |---| ``cdap-etl-batch``, ``data-pipeline``, and
``cdap-etl-realtime``, referred to as system artifacts |---| which can be used to create
different kinds of applications that work in either batch (``cdap-etl-batch``,
``data-pipeline``) or real-time (``cdap-etl-realtime``). They work with a collection of
sources, transformations, sinks, and other plugins, either those that are packaged as part
of CDAP or ones that have been installed separately.

An additional system artifact (``cdap-etl-lib``) provides common resources for the other
system artifacts, and can be used by developers of custom plugins.

Pipelines can be created using Cask Hydrator's included visual editor (*Cask Hydrator
Studio*), using command-line tools such the CDAP CLI and curl, or programmatically with
scripts or Java programs.

Pipeline Lifecycle
------------------
- cover

User "workflow" to create and run a pipeline

CDAP steps to deploy and run a pipeline.



.. rubric:: **Sidebar:** *What is ETL?*

.. container:: inline-sidebar

  Most data infrastructures are front-ended with an ETL system (Extract-Transform-Load). The
  purpose of the system is to:

  - Extract data from one or more sources;
  - Transform the data to fit the operational needs; and
  - Load the data into a desired destination.

  ETL usually comprises a source, zero or more transformations, and one or more sinks, in
  what is called an ETL pipeline:

  .. image:: _images/etl-pipeline.png
     :width: 6in
     :align: center

  Any of the pipelines created with Hydrator can be used for ETL.

.. _cask-hydrator-introduction-what-is-a-plugin:

What is a Plugin?
=================
Sources, transformations (called *transforms* for short), and sinks are generically called
*plugins*. Plugins provide a way to extend the functionality of existing artifacts. An
application can be created with the existing plugins included with CDAP or, if a user
wishes, they can write a plugin to add their own capability.

Some plugins |---| such as the *JavaScript*, *Python Evaluator*, and *Validator*
transforms |---| are designed to be customized by end-users with their own code from
within Hydrator Studio. With those, you can create your own data validators either by
using the functions supplied in the CoreValidator plugin or by implementing and supplying
your own custom validation function.

Types of Plugins
----------------
These are the basic plugin types in |cdap-hydrator-version|:

- Actions
- Batch Source
- Batch Sink
- Real-time Source
- Real-time Sink
- Transformation (Transform)
- Aggregator
- Compute
- Model
- Shared
- Post-action (called after the pipeline has run)

Additional types of plugins are under development, and developers can create and
add their own plugins and plugin types.

The batch sources can write to any batch sinks that are available and real-time sources
can write to any real-time sinks. Transformations work with either sinks or sources.
Transformations can use validators to test data and check that it follows user-specified
rules. Other plugin types may be restricted as to which plugin (and artifact) that they
work with, depending on the particular functionality they provide.

For instance, certain model (*NaiveBayesTrainer*) and compute (*NaiveBayesClassifier*) plugins
only work with batch pipelines.

Action plugins (supported only in pipelines based on the ``data-pipeline`` artifact) can
be added to run either before a source or after a sink. A "post-action" plugin can be
specified that runs after the entire pipeline has run.

A reference lists and describes all :ref:`plugins included with CDAP <cask-hydrator-plugins>`.

Creating Plugins
----------------
Developers are free to create and add not only their own custom plugins, but their own plugin types.
Details on plugin creation are covered in :ref:`cask-hydrator-creating-custom-plugins`.
 
Plugin Templates
----------------
Within :ref:`Hydrator Studio <cask-hydrator-introduction_hydrator_studio>`, you can create
plugin templates: customized versions of a plugin that are reusable, and can contain
pre-configured settings.

Setting can be locked so that they cannot be altered when they are eventually used.

Once a plugin template has been created, it can be edited and deleted at a later time.

Changes to a plugin template do not affect any pipelines created using that template, as
those pipelines are created from the artifacts as specified in the plugin template at the
time of creation.


What is a Structured Record?
============================
Each stage of a pipeline that emits data (basically, all stages except for *actions* and
*sinks*) emits in the form of a ``Structured Record``, which can be thought of simply as
data, in the form of a map of key-value pairs, with an included schema that describes the
record. In general, sinks usually accept a ``Structured Record``, and sources often
(though not always) emit them. There are transform plugins that convert either to or from
Structured Records to other formats as required.


What are Properties?
====================
Each stage in a pipeline represents the configuration of a specific plugin, and that
configuration requires certain properties. At a minimum, a unique name for the stage and the
plugin being used is required, with any additional properties required dependent on the
particular plugin used.

See the :ref:`reference section <cask-hydrator-plugins>` for details on the properties
required and supported for each plugin.


.. _cask-hydrator-introduction_hydrator_studio:

Hydrator Studio
===============
Hydrator supports end-users with self-service batch and real-time data ingestion combined
with ETL (extract-transform-load), expressly designed for the building of Hadoop data
lakes and data pipelines. Called *Cask Hydrator Studio*, it provides for CDAP users a
seamless and easy method to configure and operate pipelines from different types of
sources and data using a visual editor.

You drag and drop sources, transformations, sinks, and other plugins to configure a pipeline:

.. figure:: _images/hydrator-studio.png
   :figwidth: 100%
   :width: 6in
   :align: center
   :class: bordered-image-top-margin

   **Cask Hydrator Studio:** Visual editor showing the creation of an ETL pipeline

Once completed, Hydrator provides an operational view of the resulting pipeline that allows for
monitoring of metrics, logs, and other runtime information:

.. figure:: _images/hydrator-pipelines.png
   :figwidth: 100%
   :width: 6in
   :align: center
   :class: bordered-image

   **Cask Hydrator Pipelines:** Administration of created pipelines showing their current status
   


Hydrator Studio Tips
--------------------
*[Note: this section may be best elsewhere, but for now this will keep it in a visible location.]*

- After clicking on a node, a dialog comes up to allow for configuring of the node. This
  dialog can be closed either with the close button or the *escape* key.
  
- When a connection has been made from one node to a target node, you can remove the
  connection by grabbing the end with the arrow symbol and dragging it off the target node.

- All pipelines must have unique names, and a pipeline **cannot be saved over an existing
  pipeline** of the same name. Instead, increment the name (from *Demo* to *Demo-1*) with
  each new cloning of a pipeline.
