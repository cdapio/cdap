.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _cask-hydrator-creating-pipelines:

==================
Creating Pipelines
==================

There are two different ways of creating pipelines:

- Using Hydrator Studio
- Using command line tools (such as the CDAP CLI or ``curl``)

Using **Hydrator Studio,** the basic set of operations is:

- **Create** a new pipeline, either by starting from a blank canvas, starting from a
  template, or cloning an already published pipeline.

- **Edit** the pipeline in Hydrator Studio, setting appropriate configurations and
  settings.

- **Save** the pipeline as you are working in it, as a draft pipeline, using a unique name.

- **Validate** the pipeline from within Hydrator Studio, to check that basic settings and
  naming are correct.

- **Publish** the pipeline from within Hydrator Studio, which will translate the virtual
  pipeline of the configuration into a physical pipeline with the specified name.
  
At this point, the pipeline can be run, either from within Hydrator or CDAP.

Using **command line tools,** the basic set of operations is:

- **Create** a new pipeline by writing a configuration file, in JSON format following the
  Hydrator configuration specification, either from an empty configuration, starting with an
  example or template, or re-using an existing configuration file.

- **Edit** the JSON configuration file in an editor, setting appropriate configurations and
  settings.

- **Publish** the pipeline either by using the Lifecycle RESTful API or CDAP CLI, which
  will translate the virtual pipeline of the configuration file into a physical pipeline
  with the specified name.
  
Pipelines published using command line tools are visible within both CDAP and Hydrator, and
can be cloned and edited using Hydrator Studio.

**Note:** Unlike many editors, Hydrator Studio does not allow draft pipelines to be published
"on top of" existing, published pipelines, as this could invalidate existing logs, metrics,
and datasets. Instead, it requires you to create a new name for any newly-published pipelines.


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

- :ref:`Batch Aggregator Plugins <cask-hydrator-plugins-batch-aggregators>`

- :ref:`Batch Compute Plugins <cask-hydrator-plugins-batch-computes>`

- :ref:`Batch Model Plugins <cask-hydrator-plugins-batch-models>`

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

Post-run Actions
----------------
Post-run actions can be configured for a batch pipeline, either by using the Hydrator Studio or
by setting the "postRunActions" property of the configuration JSON file. The available
actions are determined by the post-run plugins that are available to the application
template being used to create the pipeline.

Currently, post-run plugins are only available when using the ``cdap-data-pipeline``
application template.

Available post-run plugins are documented in the :ref:`reference of plugins
<cask-hydrator-plugins-post-run-plugins>`.

Real-time Pipelines
===================

Introduction
------------
Real-time pipelines are designed to poll sources periodically to fetch data,
perform any (optional) transformations, and then write to one or more real-time sinks.

Types of Plugins
----------------
Real-time pipelines, based on the ``cdap-etl-realtime`` application template, can include these plugins:

- :ref:`Actions <cask-hydrator-action-plugins>`

- :ref:`Real-time Sink Source Plugins <cask-hydrator-plugins-realtime-sources>`

- :ref:`Real-time Transformation Plugins <cask-hydrator-plugins-realtime-transformations>`

- :ref:`Real-time Sink Plugins <cask-hydrator-plugins-realtime-sinks>`

How Does It Work?
-----------------
The realtime pipeline is created by taking a "virtual" pipeline (in the form of a
configuration file) and then creating a "physical" pipeline as a CDAP application with
appropriate CDAP programs to implement the configuration.

The programs used will depend on the plugins used to build the pipeline. The available
plugins are determined by those plugins that will work with the *ETL Realtime* (the
``cdap-etl-realtime`` artifact), and listed as :ref:`real-time plugins
<cask-hydrator-plugins-realtime>`.

The application created will consist of a worker to be run continuously, polling as required.

Building a Pipeline
-------------------
To create a real-time pipeline, you can use either Hydrator Studio or command line tools.

To use Hydrator Studio to create a real-time pipeline:

- Specify *ETL Realtime* (the ``cdap-etl-realtime`` artifact) as the application
  template for your pipeline.

- Click the icons in the left-sidebar to select the plugins you would like included in
  your pipeline. In addition to the :ref:`action plugins <cask-hydrator-action-plugins>`,
  you can use any of the :ref:`real-time plugins <cask-hydrator-plugins-realtime>`.

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
<hydrator-developing-pipelines-creating-realtime>`.



Common Settings
===============
These settings can be used in both batch and real-time pipelines.

Configuring Resources
---------------------
*[New in 3.5: To Be Completed]*

.. _cask-hydrator-runtime-arguments-macros:

Macro Substitution
------------------
To handle the problem of configuring a pipeline, but not knowing at the time of
configuration the value of a parameter until the actual runtime, you can use macros.

Macros are set using a syntax of ``${macro-name}``, where ``macro-name`` is a key in the
preferences (or in the runtime arguments) for the physical pipeline.

For instance, you might not know the name of a source stream until runtime. You could use,
in the source stream's *Stream Name* configuration::

  ${source-stream-name}
  
and in the preferences (or the runtime arguments) set a key-value pair such as::

  source-stream-name: myDemoStream
  
Macros can be referential. You might have an server that refers to a hostname and port,
and supply this preference::

 server-address: ${hostname}:${port}

and these runtime arguments::
 
 hostname: my-demo-host.example.com
 port: 9991
 
In a pipeline configuration, you could use an expression with::

  ${server-address}

expecting that it would be replaced with::

  my-demo-host.example.com:9991
  
Information on setting preferences and runtime arguments is in the :ref:`CDAP
Administration Manual, Preferences <preferences>`. These can be set with the HTTP
:ref:`Lifecycle <http-restful-api-lifecycle-start>` and :ref:`Preferences
<http-restful-api-preferences>` RESTful APIs.


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


Using and Re-using Existing Pipelines
=====================================
Existing pipelines can be used to create new pipelines by:

- cloning an already-published pipeline and saving the resulting draft with a new name; or
- exporting a configuration file, editing it, and then importing the revised file.


Cloning
-------
Any existing pipeline that has been published, can be *cloned.* This 
creates an in-memory copy of the pipeline with the same name and opens it within Hydrator Studio.

At this point, you can rename the pipeline to a unique name and then save it as draft.


Pipeline Drafts *(Hydrator Studio)*
-----------------------------------
From within *Hydrator Studio*, you can save a pipeline you are working on at any time as
a *draft*. The pipeline configuration is saved, and you can resume editing later.

To create a draft, give your pipeline a unique name, and then click the *Save* button:

.. figure:: /_images/hydrator-gs-1-5-buttons.png
  :figwidth: 100%
  :width: 6in
  :align: center
  :class: bordered-image

  **Cask Hydrator Studio:** Button labels, upper-right toolbar

The draft will be created, and will show in your list of pipelines as a draft. 
Clicking on it in the list of pipelines will re-open it in *Hydrator Studio* so that 
you can continue working on it.

Note that if you change the name of draft, it doesn't create a new draft with the new name, but
simply renames the existing draft. Names of drafts must be unique, and names of published pipelines
must be unique, though you can have a draft that is the same name as a published pipeline.

To successfully publish such a draft, you will need to re-name it to a unique name.


Creating Plugin Templates
-------------------------

From within Hydrator Studio, you can create a **plugin template,** a variation
of a plugin that you can configure with particular settings for re-use. 

To create a plugin-template:

- From within Hydrator Studio, hover your mouse over the plugin you would like to use
  for your template, such as the *Stream* source plugin.

- In the on-hover menu that appears, click the "+ Template" button.

- The window that appears will allow you to specify the version of the plugin to use. Once
  you do, the window will expand to allow you to specify the particular properties of that
  plugin.

- The template will require a name that uniquely identifies it. 

- You can lock individual properties of the configuration so that they are not editable
  when the template is used.

- When the plugin template is successfully saved, it will appear in with the other plugins, with
  an additional "T" icon to indicate that it is a template.

- Templates can be either edited or deleted after they are created, using buttons that
  will appear in their on-hover menu.

Once created, you can use the plugin template just as you would any other plugin, with the
advantage that it can be pre-configured with settings that you re-use or require.

Preconfigured Pipelines
-----------------------
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

Then, click "Template Gallery" to bring up a dialog that shows the available templates.
Click on which ever one you'd like, and it will open, allowing you to begin 
customizing it to your requirements.

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
  
- **ETL Batch** (deprecated; use *Data Pipeline* instead)

  - **Stream to HBase:** Periodically ingest from a stream into an HBase table


Importing *(Hydrator Studio)*
-----------------------------
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


Exporting
-----------------------------
There are two ways you can export a pipeline configuration file:

1. From with Hydrator Studio; and
#. From within a Hydrator pipeline configuration page.

1. From within Hydrator Studio, you can export a pipeline configuration JSON file using
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
   similar. The UI information is added in the "__ui__" object in the JSON configuration
   file.

#. From within a Hydrator pipeline configuration page, there is an *Export* button:

   .. figure:: /_images/hydrator-pipeline-detail-configuration.png
      :figwidth: 100%
      :width: 6in
      :align: center
      :class: bordered-image
 
      **Cask Hydrator:** Configuration page, pipeline detail, showing *Export* button on right
      
      Similar to exporting from with Hydrator Studio, exporting using the button will
      produce a configuration with UI information, and copying the configuration visible
      in the lower portion of the page will produce a configuration that does not include
      the "__ui__" object in the JSON.

Files created by exporting can be edited in a text editor and then imported to create new pipelines.
