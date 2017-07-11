.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016-2017 Cask Data, Inc.

.. _cdap-studio:

===========
CDAP Studio
===========

CDAP supports end-users with self-service batch and real-time data ingestion combined
with ETL (extract-transform-load), expressly designed for the building of Hadoop data
lakes and data pipelines. Called the *CDAP Studio*, it provides for CDAP users a
seamless and easy method to configure and operate pipelines from different types of
sources and data using a visual editor.

You click and drag *actions*, *sources*, *transforms*, *sinks*, and other plugins to
configure a pipeline:

.. figure:: _images/cdap-studio.png
   :figwidth: 100%
   :width: 6in
   :align: center
   :class: bordered-image-top-margin

   **CDAP Studio:** Visual editor showing the creation of a pipeline

Once completed, CDAP provides an operational view of the resulting pipeline that allows for
monitoring of metrics, logs, and other runtime information:

.. figure:: _images/cdap-pipelines.png
   :figwidth: 100%
   :width: 6in
   :align: center
   :class: bordered-image

   **CDAP Pipelines:** Administration of created pipelines showing their current status

CDAP Studio Tips
================
- Once you have **specified an application template** for your pipeline and have started
  adding stages, it cannot be changed for that pipeline. This is because the selection of
  the application template determines which plugins are available for the pipeline.
  
- After clicking on a node, a dialog comes up to allow for **configuring of the node**. As any
  changes are automatically saved, you can just close the dialog by either hitting the close
  button (an *X* in the upper-right corner), the *escape* key on your keyboard, or clicking
  outside the dialog box.
  
- To **edit a connection** made from one node to another node, you can remove the
  connection by clicking the end with the arrow symbol (click on the white dot) and dragging
  it off of the target node.

- All **pipelines must have unique names**, and a pipeline **cannot be saved over an existing
  pipeline** of the same name. Instead, increment the name (from *Demo* to *Demo-1*) with
  each new cloning of a pipeline.


.. _cdap-studio-pipeline-drafts:

Pipeline Drafts
===============
From within the *CDAP Studio*, you can save a pipeline you are working on at
any time as a *draft*. The pipeline configuration is saved, and you can resume editing
later.

To create a draft, give your pipeline a unique name, and then click the *Save* button:

.. figure:: _images/cdap-pipelines-gs-1-5-buttons.png
  :figwidth: 100%
  :width: 6in
  :align: center
  :class: bordered-image

  **CDAP Studio:** Button labels, upper-right toolbar

The draft will be created, and will show in your list of pipelines as a draft. 
Clicking on it in the list of pipelines will re-open it in the *CDAP Studio* so that 
you can continue working on it.

Note that **if you change the name of draft, it doesn't create a new draft** with the new
name, but simply renames the existing draft. Names of drafts must be unique, and names of
published pipelines must be unique, though you can have a draft that is the same name as a
published pipeline.

To successfully publish such a draft (one whose name matches an existing pipeline), you
will need to re-name it to a unique name.

.. _cdap-studio-plugin-templates:

Plugin Templates
================
Within the CDAP Studio, you can create *plugin templates:* customized versions of a plugin
that are reusable, and can contain pre-configured settings.

Setting can be locked so that they cannot be altered when they are eventually used.

Once a plugin template has been created, it can be edited and deleted at a later time.

Changes to a plugin template do not affect any pipelines created using that template, as
those pipelines are created from the artifacts as specified in the plugin template at the
time of creation of the pipeline.

.. figure:: _images/cdap-studio-plugin-template.png
  :figwidth: 100%
  :width: 6in
  :align: center
  :class: bordered-image

  **CDAP Studio:** Creating a plugin template from the Stream source plugin

Creating a Plugin Template
--------------------------
To create a plugin-template:

- From within the CDAP Studio, hover your mouse over the plugin you would like to use
  for your template, such as the *Stream* source plugin.

- In the on-hover menu that appears, click the *+ Template* button.

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
