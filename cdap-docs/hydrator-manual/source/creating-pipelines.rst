.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _cask-hydrator-creating-pipelines:

==================
Creating Pipelines
==================

Pipelines are created from *artifacts*. A number of artifacts are supplied with CDAP, and
custom artifacts can be created by developers. An artifact is a blueprint or template that
|---| with the addition of a configuration file |---| is used to create an application.

A pipeline application is created by preparing a configuration that specifies the artifact
and which source, transformations (also known as transforms), and sinks are to be used to
create the application.

The sources, transformations, and sinks are packaged as extensions to CDAP known as
**plugins**, and can include actions to be taken at the start of pipeline run, at the end
of the run, and after the run has been completed. The plugins can be either those that are
packaged as part of CDAP or ones that have been installed separately.

The configuration can either be written as a JSON file or, in the case of the Hydrator
Studio, specified in-memory.

CDAP currently provides two artifacts (referred to as *system artifacts*):

- ``cdap-data-pipeline`` (for batch pipelines)
- ``cdap-etl-realtime`` (for realtime pipelines)

which are used to create the different kinds of data pipeline applications. (**Note:** *A third
system artifact,* ``cdap-etl-batch`` *has been deprecated and replaced by the*
``cdap-data-pipeline`` *artifact, as of CDAP 3.5.0.*)

An additional system artifact (``core-plugins``) provides common resources for the other
system artifacts, and can be used by developers of custom plugins.

Pipelines can be created using Cask Hydrator's included visual editor (*Cask Hydrator
Studio*), using command-line tools such the CDAP CLI and ``curl``, or programmatically
with scripts or Java programs.


Methods for Creating Pipelines
==============================
There are two different methods for creating pipelines:

1. Using Hydrator Studio
#. Using command line tools (such as ``curl``, the CDAP CLI, or the CDAP UI)

Using **Hydrator Studio,** the basic operations are:

  1. **Create** a new pipeline, either by starting from a :ref:`blank canvas <cask-hydrator-getting-started-hydrator-studio>`, 
     starting from a `template <Pipeline Templates>`_, or by `cloning <Cloning>`_ an already-published pipeline.

  #. **Edit** the pipeline in Hydrator Studio, setting appropriate configurations and
     settings.

  #. **Save** the pipeline as you are working on it, as a draft pipeline, using a unique name.

  #. **Validate** the pipeline from within Hydrator Studio, to check that basic settings and
     naming are correct.

  #. **Publish** the pipeline from within Hydrator Studio, which will translate the virtual
     pipeline of the configuration into a physical pipeline with the specified name.
  
  At this point, the pipeline can be run, either from within Hydrator or CDAP.

  **Note:** Unlike many editors, Hydrator Studio does not allow draft pipelines to be
  published "on top of" existing, published pipelines, as this would invalidate existing
  logs, metrics, and datasets. Instead, it requires you to create a new name for any
  newly-published pipelines.

Using **command line tools,** the basic operations are:

  1. **Create** a new pipeline by writing a configuration file, in JSON format following the
     :ref:`Hydrator configuration specification <hydrator-developing-pipelines-configuration-file-format>`, 
     either from an empty configuration, starting with an example or template, or re-using an
     existing configuration file.

  #. **Edit** the JSON configuration file in an editor, setting appropriate configurations and
     settings.

  #. **Publish** the pipeline either by using the Lifecycle RESTful API or CDAP CLI, which
     will translate the virtual pipeline of the configuration file into a physical pipeline
     with the specified name.
   
  Pipelines published using command line tools are visible within both CDAP and Hydrator, and
  can be cloned and edited using Hydrator Studio.


Batch Pipelines
===============

Introduction
------------
Batch pipelines can be scheduled to run periodically, using a cron expression and can read
data from batch sources using either a MapReduce or Spark job. The batch application then
performs any (optional) transformations before writing to one or more batch sinks.

Action plugins can be added to perform special actions before the pipeline starts, and
when it successfully finishes. Post-run actions can be specified that will always run,
irregardless if the pipeline successfully completed.

Types of Plugins
----------------
Batch pipelines, based on the ``cdap-data-pipeline`` application template, can include these plugins:

- :ref:`Actions <cask-hydrator-action-plugins>`

- :ref:`Batch Source Plugins <cask-hydrator-plugins-batch-sources>`

- :ref:`Batch Transformation Plugins <cask-hydrator-plugins-batch-transformations>`

- :ref:`Batch Sink Plugins <cask-hydrator-plugins-batch-sinks>`

How Does It Work?
-----------------
The batch pipeline is created by taking a "virtual" pipeline (in the form of a
configuration file) and then creating a "physical" pipeline as a CDAP application with
appropriate CDAP programs to implement the configuration.

The programs used will depend on the engine chosen (MapReduce or Spark) and the plugins
used to build the pipeline. The available plugins are determined by those plugins that will
work with the *Data Pipeline* (the ``cdap-data-pipeline`` artifact), and listed
as :ref:`batch plugins <cask-hydrator-plugins-batch>`.

Building a Pipeline
-------------------
To create a batch pipeline, you can use either command line tools or Hydrator Studio.

To use Hydrator Studio to create a batch pipeline:

- Specify *Data Pipeline* (the ``cdap-data-pipeline`` artifact) as the application
  template for your pipeline.

- Click the icons in the left-sidebar to select the plugins you would like included in
  your pipeline. In addition to the :ref:`action plugins <cask-hydrator-action-plugins>`,
  you can use any of the :ref:`batch plugins <cask-hydrator-plugins-batch>`.

- Typically, you will need at a minimum a source, a sink, and any optional transformations
  that are needed being the source and sink stages.
  
- Action steps can be added before a source and after a sink. These will be run only at
  the start (before a source) and only at the end if the pipeline successfully completes.

- The *Settings* button allows you to specify the *Schedule*, "Post-run Actions* and *Engine* used
  for the pipeline.

- Specify a schedule for the batch pipeline, using either the *basic* or *advanced* specification.
  The schedule uses the underlying operating system's ``cron`` application.

- :ref:`Post-run actions <cask-hydrator-plugins-post-run-plugins>` can be specified, and
  these will be run depending on the configuration; they can run even if the pipeline fails,
  as they can be specified to run on one of *completion*, *success*, or *failure*. You can
  have any number of post-run actions, and additional ones are added by clicking the *+*
  button.
  
- Specify an engine to use for the CDAP application. By default, *MapReduce* is used.

- Complete all required information for each stage, and any optional information that your
  particular use requires.

- Save the pipeline under a unique name

- Validate the pipeline, to check for errors.

- Publish the pipeline, which will turn the virtual pipeline of the configuration file
  into a physical pipeline of CDAP programs in a CDAP application.
  
Note that publishing a pipeline can reveal errors that the validation step doesn't catch, as
validation is not an exhaustive test.

At this point you can run your pipeline, either from within Hydrator or from within CDAP.

Details and an example of using command line tools to create a batch pipeline are in the
section on :ref:`developing pipelines: creating a batch pipeline
<hydrator-developing-pipelines-creating-batch>`.

Scheduling
----------
From with Hydrator Studio, you can set a schedule for a batch pipeline that will be used to run it. Note that as
a schedule is set as part of the pipeline configuration, a physical pipeline's schedule cannot be altered except by
creating a new pipeline with a new schedule.

Two interfaces are available: 

- A *basic* interface, where you select the time increment (every minute, hour, day, week,
  month, year) and the amount after the increment, as appropriate:

  +-------+-----------------------------------------------------------------------------------------------------------------------+
  | Hour  | Five-minute increment after the hour, 0 through 55 minutes                                                            |
  +-------+-----------------------------------------------------------------------------------------------------------------------+
  | Day   | Hour (twenty-four hour clock), plus five-minute increment after the hour, 0 through 55 minutes                        |
  +-------+-----------------------------------------------------------------------------------------------------------------------+
  | Week  | Day of the week, plus hour (twenty-four hour clock), plus five-minute increment after the hour, 0 through 55 minutes  |
  +-------+-----------------------------------------------------------------------------------------------------------------------+
  | Month | Day of the month, plus hour (twenty-four hour clock), plus five-minute increment after the hour, 0 through 55 minutes |
  +-------+-----------------------------------------------------------------------------------------------------------------------+
  | Year  | Date, plus hour (twenty-four hour clock), plus five-minute increment after the hour, 0 through 55 minutes             |
  +-------+-----------------------------------------------------------------------------------------------------------------------+

  If the specified time does not exist (for instance, you specified the 31st day of the
  month, which doesn't occur in June), the event is skipped until the next occurring event.
  
  This *basic* schedule is converted into a ``cron`` expression for the configuration file.

- An *advanced* interface, which provides you access to the same interface as used in the
  underlying ``cron`` program. The details of that program will depend on the operating
  system used by the host of the CDAP Master process.

Engine
------
You can specify the engine being used for a batch pipeline, either "MapReduce" (``mapreduce``)
or "Spark" (``spark``).

You set this either by selecting your choice using the *Settings* tool of Hydrator Studio,
or by setting the engine property in the configuration file for the pipeline::

    "engine": "mapreduce",

This determines the particular engine that will be used when the physical pipeline is
created.

.. _cask-hydrator-creating-pipelines-actions:

Actions
-------
Actions can be configured for a batch pipeline, either by using the Hydrator Studio or by
including a stage of type ``action`` in the configuration JSON file. The available actions
are determined by those available to the application template being used to create the
pipeline.

If configured, the action takes place either at the start or at the completion of a
pipeline run. All actions configured for the start will complete first before any other
stages, and all other stages will complete before any of the actions at the end are run.

Actions at the end will only run if the pipeline successfully completes. If you need an
action to run irregardless of completion, use a :ref:`post-run action
<cask-hydrator-creating-pipelines-post-run-actions>` instead.

Currently, action plugins are only available when using the ``cdap-data-pipeline``
application template. Available action plugins are documented in the :ref:`Plugin
Reference <cask-hydrator-action-plugins>`, with this action available:

- *SSH Action*, which establishes an SSH connection with a remote machine to execute a
  command on that machine.

.. _cask-hydrator-creating-pipelines-post-run-actions:

Post-run Actions
----------------
Post-run actions can be configured for a batch pipeline, either by using the Hydrator Studio or
by setting the "postActions" property of the configuration JSON file. The available
actions are determined by the post-run plugins that are available to the application
template being used to create the pipeline.

If configured, the actions take place after the completion of a pipeline run,
and can happen depending of the status of the run. One of three conditions must be specified:

- completion (action takes place regardless of the status)
- success (action takes place only upon success)
- failure (action takes place only upon failure)

Currently, post-run plugins are only available when using the ``cdap-data-pipeline``
application template. Available post-run plugins are documented in the :ref:`Plugin Reference
<cask-hydrator-plugins-post-run-plugins>`, with these actions currently available:

- sending an email
- running a database query
- making an HTTP request


Real-time Pipelines
===================

Introduction
------------
Real-time pipelines are designed to poll sources periodically to fetch data, perform any
(optional) transformations, and then write to one or more real-time sinks. As they are
intended to be run continuously, post-run actions are not applicable or available.

Types of Plugins
----------------
Real-time pipelines, based on the ``cdap-etl-realtime`` application template, can include these plugins:

- :ref:`Actions <cask-hydrator-action-plugins>`

- :ref:`Real-time Sink Source Plugins <cask-hydrator-plugins-real-time-sources>`

- :ref:`Real-time Transformation Plugins <cask-hydrator-plugins-real-time-transformations>`

- :ref:`Real-time Sink Plugins <cask-hydrator-plugins-real-time-sinks>`

How Does It Work?
-----------------
A real-time pipeline is created by taking a "virtual" pipeline (in the form of a
configuration file) and then creating a "physical" pipeline as a CDAP application with
appropriate CDAP programs to implement the configuration.

The programs used will depend on the plugins used to build the pipeline. The available
plugins are determined by those plugins that will work with the *ETL Realtime* (the
``cdap-etl-realtime`` artifact), and listed as :ref:`real-time plugins
<cask-hydrator-plugins-real-time>`.

The application created will consist of a worker to be run continuously, polling as required.

Building a Pipeline
-------------------
To create a real-time pipeline, you can use either Hydrator Studio or command line tools.

To use Hydrator Studio to create a real-time pipeline:

- Specify *ETL Realtime* (the ``cdap-etl-realtime`` artifact) as the application
  template for your pipeline.

- Click the icons in the left-sidebar to select the plugins you would like included in
  your pipeline. In addition to the :ref:`action plugins <cask-hydrator-action-plugins>`,
  you can use any of the :ref:`real-time plugins <cask-hydrator-plugins-real-time>`.

- Typically, you will need at a minimum a source, a sink, and any optional transformations
  that are needed being the source and sink stages.
  
- Action steps can be added before a source and after a sink. These will be run only at
  the start (before a source) and only at the end if the pipeline successfully completes.

- The *Settings* button allows you to specify the number of instances used for workers of
  the pipeline. The default is one.

- Complete all required information for each stage, and any optional information that your
  particular use requires.

- Save the pipeline under a unique name

- Validate the pipeline, to check for errors.

- Publish the pipeline, which will turn the virtual pipeline of the configuration file
  into a physical pipeline of CDAP programs in a CDAP application.
  
Note that publishing a pipeline can reveal errors that the validation step doesn't catch, as
validation is not an exhaustive test.

At this point you can run your pipeline, either from within Hydrator or from within CDAP.

Details and an example of using command line tools to create a real-time pipeline are in the
section on :ref:`developing pipelines: creating a real-time pipeline
<hydrator-developing-pipelines-creating-real-time>`.


Common Configuration Settings
=============================
These settings can be used in both batch and real-time pipelines.

Required Fields
---------------
Certain fields are required to be configured in order for the plugin to work. These are
identified in the Hydrator Studio configuration panel by a red dot, and are
described in the :ref:`Hydrator Plugin Reference <cask-hydrator-plugins>`
documentation as *required*.

.. Configuring Resources
.. ---------------------

.. _cask-hydrator-runtime-arguments-macros:

Macro Substitution
------------------
To handle the problem of configuring a pipeline, but not knowing at the time of
configuration the value of a parameter until the actual runtime, you can use macros.

Macros are set using a syntax of ``${macro-name}``, where ``macro-name`` is a key in the
preferences (or in the runtime arguments or the workflow token) for the physical pipeline.

For instance, you might not know the name of a source stream until runtime. You could use,
in the source stream's *Stream Name* configuration::

  ${source-stream-name}
  
and in the runtime arguments set a key-value pair such as::

  source-stream-name: myDemoStream
  
Macros can be referential (refer to other macros), up to ten levels deep. For instance,
you might have an server that refers to a hostname and port, and supply these runtime
arguments, one of which is a definition of a macro that uses other macros::
 
  hostname: my-demo-host.example.com
  port: 9991
  server-address: ${hostname}:${port}
 
In a pipeline configuration, you could use an expression such as::

  server-address: ${server-address}

expecting that it would be replaced with::

  my-demo-host.example.com:9991

The order of precedence (from lowest to highest) for resolving macros is::

  Preferences < Runtime Arguments < Workflow Token
  
This order is used so that the most volatile source (the workflow token) takes precedence.

Information on setting preferences and runtime arguments is in the :ref:`CDAP
Administration Manual, Preferences <preferences>`. These can be set with the HTTP
:ref:`Lifecycle <http-restful-api-lifecycle-start>` and :ref:`Preferences
<http-restful-api-preferences>` RESTful APIs.

Fields that are macro-enabled are identified in the Hydrator Studio UI and documented in
the :ref:`Hydrator Plugin Reference <cask-hydrator-plugins>`.


Macro Functions
---------------
In addition to macro substitution, you can use pre-defined macro functions. Currently,
these functions are predefined and available:

- ``logicalStartTime``
- ``secure``

.. |SimpleDateFormat| replace:: Java ``SimpleDateFormat``
.. _SimpleDateFormat: http://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html

Logical Start Time Function
...........................
The logicalStartTime macro function returns the logical start time of a run of the pipeline.

If no parameters are supplied, it returns the start time in milliseconds.
All parameters are optional. The function takes a time format, an offset, and a timezone as
arguments and uses the logical start time of a pipeline to perform the substitution::

  ${logicalStartTime([timeFormat[,offset [,timezone])}
  
where

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``timeFormat`` *(Optional)*
     - Time format string, in the format of a |SimpleDateFormat|
   * - ``offset`` *(Optional)*
     - Offset from the before the logical start time
   * - ``timezone`` *(Optional)*
     - Timezone to be used for the logical start time

For example, suppose the logical start time of a pipeline run is ``2016-01-01T00:00:00`` and
this macro is provided::

  ${logicalStartTime(yyyy-MM-dd'T'HH-mm-ss,1d-4h+30m)}

The format is ``yyyy-MM-dd'T'HH-mm-ss`` and the offset is ``1d-4h+30m`` before the logical
start time. This means the macro will be replaced with ``2015-12-31T03:30:00``, since the
offset translates to 20.5 hours. The entire macro evaluates to 20.5 hours before midnight
of January 1 2016.

Secure Function
...............
The secure macro function takes in a single key as an argument and looks up the key's
associated string value from the Secure Store. In order to perform the substitution, the
key provided as an argument must already exist in the secure store. This is useful for
performing a substitution with sensitive data.

For example, for a plugin that connects to a MySQL database, you could configure the
*password* property field with::

  ${secure(mysql-password)}

which will pull the *mysql-password* from the Secure Store at runtime.


Validation
==========
From within Hydrator Studio, the validation button will check the pipeline from within
Hydrator Studio, to check that basic settings and naming are correct. Messages of any
errors found will be shown in Studio. Note that this step is not exhaustive, and errors
may still be found when the pipeline is actually published.


Publishing
==========
Publishing a pipeline takes a *virtual* pipeline (such as a draft in Hydrator Studio, or a
configuration JSON file) and creates a *physical* pipeline (a CDAP application) using the
configuration file, plugin artifacts, and application template artifacts.

Publishing can happen either from with Hydrator Studio or by using command line tools, 
such as the ``curl`` command with the Lifecycle RESTful API, or the CDAP CLI tool with its
``create app`` command.

Using either method, published pipelines are visible within both CDAP and Hydrator, and
can be cloned and edited using Hydrator Studio.


Templates and Re-using Pipelines
================================
Existing pipelines can be used to create new pipelines by:

- Using a **pipeline template**
- **Cloning** an already-published pipeline and saving the resulting draft with a new name
- **Exporting** a configuration file, editing it, and then **importing** the revised file

Pipeline Templates
------------------
A collection of predefined and preconfigured pipelines are available from within Hydrator
Studio through the controls at the top of the left side-bar. These templates can be used
as the starting point for either your own pipelines or your own pipeline templates.

.. figure:: /_images/hydrator-studio-annotated.png
   :figwidth: 100%
   :width: 6in
   :align: center
   :class: bordered-image

   **Cask Hydrator Studio:** Annotations showing components

First, select which application template you wish to use, either *Data Pipeline* or 
*ETL Real-time*.

Then, click *Template Gallery* to bring up a dialog that shows the available templates.
Click on the one you'd like to start with, and it will open, allowing you to begin customizing it
to your requirements.

.. These names & descriptions were extracted from cdap/cdap-ui/templates/apps/predefined/config.json

These are the available templates:

- **Data Pipeline**

  - **Model Trainer:** Train model using Naive Bayes classifier
  
  - **Event Classifier:** Classify events into spam or non-spam using a Naive Bayes model
  
  - **Log Data Aggregator:** Aggregate log data by grouping IP and HTTP Status

- **ETL Real-time**

  - **Kafka to HBase:** Ingests in real time from Kafka into an HBase table
  
  - **Kafka to Stream:** Ingests in real time from Kafka into a stream
  
  - **Kafka to OLAP Cube:** Generate an OLAP Cube in real time from Kafka
  
  - **Twitter to HBase:** Ingest real-time Twitter Stream into an HBase table
  
  - **Twitter to Stream:** Ingest real-time Twitter Stream into a stream
  
  - **Amazon SQS to HBase:** Real-time updates from Amazon Simple Queue Service into an HBase table
  
- **ETL Batch** (deprecated as of CDAP 3.5.0; use *Data Pipeline* instead)

  - **Stream to HBase:** Periodically ingest from a stream into an HBase table

Cloning
-------
Any existing pipeline that has been published, can be *cloned.* This creates an in-memory
copy of the pipeline with the same name and opens it within Hydrator Studio.

At this point, you can rename the pipeline to a unique name and then either save it as a
:ref:`draft <cask-hydrator-studio-pipeline-drafts>` or publish it as a new pipeline. As
you cannot save over an existing pipeline, all new pipelines need a unique name; a common
practice is to increment the names, from *Demo-1* to *Demo-2* with each new clone. 

Exporting
---------
There are two ways you can export a pipeline configuration file:

1. From within Hydrator Studio; and
#. From within a Hydrator pipeline configuration page.

1. From **within Hydrator Studio**, you can export a pipeline configuration JSON file using
   the *Export...* button:

   .. figure:: /_images/hydrator-gs-1-5-buttons.png
      :figwidth: 100%
      :width: 6in
      :align: center
      :class: bordered-image
 
      **Cask Hydrator Studio:** Button labels, upper-right toolbar
    
   Clicking the "Export..." button will bring up the export dialog:
 
   .. figure:: /_images/hydrator-studio-export.png
      :figwidth: 100%
      :width: 6in
      :align: center
      :class: bordered-image
 
      **Cask Hydrator Studio:** Export dialog, with display of configuration file
    
   There are two similar actions you take. If you copy the text in the dialog and then
   paste it into a text editor, you will have a JSON file that is the configuration of the
   pipeline, but without the Hydrator Studio UI information, such as the icon locations.

   If you use the "Export" button, it will prompt for a file location before saving a
   complete file with all the information required to recreate the pipeline in Hydrator
   Studio, including details such as icon location. Otherwise, the two exports are
   similar. The UI information is added in the ``"__ui__"`` object in the JSON configuration
   file.

#. From **within a Hydrator pipeline configuration** page, there is an *Export* button:

   .. figure:: /_images/hydrator-pipeline-detail-configuration.png
      :figwidth: 100%
      :width: 6in
      :align: center
      :class: bordered-image
 
      **Cask Hydrator:** Configuration page, pipeline detail, showing *Export* button on right
      
   Similar to exporting from with Hydrator Studio, exporting using the button will
   produce a configuration with UI information, and copying the configuration visible
   in the lower portion of the page will produce a configuration that does not include
   the ``"__ui__"`` object in the JSON.

Files created by exporting can be edited in a text editor and then imported to create new pipelines.

Importing
---------
From within Hydrator Studio, you can import a pipeline configuration JSON file to create a
new pipeline using the *Import Pipeline* button:

.. figure:: /_images/hydrator-gs-1-5-buttons.png
   :figwidth: 100%
   :width: 6in
   :align: center
   :class: bordered-image

   **Cask Hydrator Studio:** Button labels, upper-right toolbar


As determined by the configuration file, the application template will be set
appropriately, and may change from the current one.

A valid configuration file that meets the Hydrator configuration file specification is
required. It may be created from an existing pipeline by exporting its configuration file.

