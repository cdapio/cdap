.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _cask-hydrator-getting-started:

===============
Getting Started
===============

This is a quick tutorial, covering the basics of Hydrator. It assumes that you are familiar with
CDAP, the concepts of streams, datasets, and applications in CDAP and basic operations
in the CDAP UI, such as entering events into a stream and exploring a dataset:

  1. `Install CDAP`_
  #. `Start Hydrator`_
  #. `Start Hydrator Studio`_
  #. `Quick Tour of Hydrator Studio`_
  #. `Creating a Simple Pipeline`_


.. _cask-hydrator-getting-started-install:

Install CDAP
============

- If you haven't already, :ref:`download and install the CDAP SDK <standalone-index>`.
- :ref:`Start CDAP <start-stop-cdap>`, open up a web browser, and go to the :cdap-ui:`CDAP UI <>`.

.. _cask-hydrator-getting-started-hydrator:

Start Hydrator
==============

- To start Hydrator, either select *Cask Hydrator* from the pull-down menu in the upper
  left, or go to the :cask-hydrator:`Cask Hydrator URL <>`.

  This will take you to a list of pipelines, which will most likely be empty:

  .. figure:: /_images/hydrator-no-pipelines.png
     :figwidth: 100%
     :width: 6in
     :align: center
     :class: bordered-image

     **Cask Hydrator:** List of Pipelines

.. _cask-hydrator-getting-started-hydrator-studio:

Start Hydrator Studio
=====================

- To start *Hydrator Studio* and start creating a pipeline, do any of these:

  - Click the *+* button to add a new pipeline;
  - Click the tool bar label *Studio* (to the left of the label *Pipelines*); or
  - Go to the :cask-hydrator-studio-artifact:`Cask Hydrator Studio <cdap-data-pipeline>` URL
  
  The Studio will start and you will be creating a new pipeline, by default the first
  pipeline type in the menu, a *Data Pipeline - Batch*, which is a batch-type pipeline:
  
  .. figure:: /_images/hydrator-studio-empty.png
     :figwidth: 100%
     :width: 6in
     :align: center
     :class: bordered-image

     **Cask Hydrator Studio:** Empty canvas for creating pipelines


.. _cask-hydrator-getting-started-studio:

Quick Tour of Hydrator Studio
=============================

Before we begin an example, let's look at the Studio and its different components:

.. figure:: /_images/hydrator-studio-annotated.png
   :figwidth: 100%
   :width: 6in
   :align: center
   :class: bordered-image

   **Cask Hydrator Studio:** Annotations showing components

In the upper-left is a menu that specifies which **application template artifact** you are
currently using for your pipeline. For now, we'll leave it with the default, *Data
Pipeline - Batch*.

In the left sidebar are icons representing the different **available plugins** that work
with the current application template. They are grouped into different categories, and are
revealed by clicking the disclosure triangles to the left of each category label
(*Source, Transform, Analytics, Sink, Action*).

In the middle is the grey, gridded **Hydrator canvas**, used to create the pipeline on by
clicking an available plugin in the left sidebar to add the plugin's icon to the canvas, and
then by dragging the icon into position.

The image shows an existing **pipeline**, with three **plugin icons** in place and the
first two connected.

Note that icons are of different **colors**, **shapes**, and different shaped **connection
nodes** (either **circular** for data connections or **square** for control connections).

- **Green:** a data **generator**, with a single **right-side** data connection node, such
  as a *source* plugin

- **Blue:** a data **receiver and generator**, with **both left- and right-side** data
  connection nodes, such as a *transform* or *analytic* plugin

- **Purple:** a data **receiver**, with only a **left-side** data connection node, such as
  a *sink* plugin

- **Brown:** an **control**, octagonal-shaped, with **both left- and right-side** control
  connection nodes, such as an *action* plugin

The small yellow circles with numerals show that there are **missing configuration
values** for the different plugins.

Between the icons are grey **connection lines**, with the arrow indicating the direction
of data flow. Solid connection lines indicate data flow; dashed connection lines indicate
control flow.

Now, let's create a pipeline!


.. _cask-hydrator-getting-started-simple:

Creating a Simple Pipeline
==========================
In this example, we'll create a pipeline that reads log file events from a source,
parses them into separate fields, and writes them as individual records to a table.

1. Start by clicking on the *Stream* source in the left panel to add a *Stream* icon to the canvas.

#. Click on the disclosure triangle to the left of the *Transform* label section to show
   the *Transforms*, and then click the *LogParser* transform to add another icon to the canvas.

#. Click on the disclosure triangle to the left of the *Sink* label section to show the
   *Sinks*, and then click the *Table* transform to add another icon to the canvas.

   Your canvas should look like this:
 
   .. figure:: /_images/hydrator-gs-1-1-icons.png
      :figwidth: 100%
      :width: 6in
      :align: center
      :class: bordered-image
 
      **Cask Hydrator Studio:** Showing icons


#. Connect the *Stream* to the *LogParser* by clicking on the green connection on the
   right-hand side of the *Stream* and dragging out to the left-side connection of the
   *LogParser* and lifting the mouse-button when you reach it. Your canvas should now look like this:

   .. figure:: /_images/hydrator-gs-1-2-connected.png
      :figwidth: 100%
      :width: 6in
      :align: center
      :class: bordered-image
 
      **Cask Hydrator Studio:** Showing connection
      
#. Similarly, connect the *LogParser* to the *Table* to complete the connections. Your
   canvas should now look like this, showing that there are a number of properties to
   be completed on each plugin:

   .. figure:: /_images/hydrator-gs-1-3-connected.png
      :figwidth: 100%
      :width: 6in
      :align: center
      :class: bordered-image
 
      **Cask Hydrator Studio:** Showing connections
      
   (If you make a mistake or need to remove a connection, click and drag on the white
   circle just to the right of the connection arrow you'd like to disconnect. When you
   drag off that circle and release the mouse, the connection will be deleted and
   disappear.) 
      
#. To set these properties, click on each icon in turn. When you click an icon, a dialog box
   comes up, showing the properties available for each plugin. Any **required** properties
   are indicated with a red asterisk after the label. In this case, the *Stream* icon has
   been clicked, and the *Label*, *Stream Name*, and *Duration* are showing as required
   properties:

   .. figure:: /_images/hydrator-gs-1-4-stream.png
      :figwidth: 100%
      :width: 6in
      :align: center
      :class: bordered-image
 
      **Cask Hydrator Studio:** Showing editing of the Stream plugin properties
      
   Enter a stream name, such as *demoStream*, and a duration, such as *1d*. As the changes
   are automatically saved, you can just close the dialog by either hitting the close button (an *X* in 
   the upper-right corner), the *escape* key on your keyboard, or clicking outside the dialog box.

#. In a similar fashion, edit the *LogParser*, setting the *Input Name* as *body*, and
   accepting the default *Log Format* of *CLF*.
   
   Edit the *Table*, and set the *Name* as *demoTable* and the *Row Field* as *ts* (for timestamp).
   
   You might notice that the schema displayed has changed as you move from the stream to
   the table; the *LogParser* modifies the schema, breaking the *body* into the separate
   fields we require in the table.
   
#. When you are finished, all icons should show that all required fields have been completed
   by the absence of yellow circles on the icons.

   Name your pipeline by clicking on the text (what else!) *Name your pipeline* above the
   canvas area, and enter a name such as *demoPipeline*. No spaces are allowed in pipeline
   names.
   
   To check that everything is complete, click the validate button, located on buttons
   above the canvas area. These controls are available:

   .. figure:: /_images/hydrator-gs-1-5-buttons.png
     :figwidth: 100%
     :width: 6in
     :align: center
     :class: bordered-image

     **Cask Hydrator Studio:** Button labels, upper-right toolbar
      
   Clicking the *Validate* button should produce a banner message similar to::
   
      Validation success! Pipeline demoPipeline is valid.

#. If there are any errors, correct them before continuing. 

   Otherwise, click the *Publish* button: the pipeline configuration will be saved; a CDAP
   application will be created, based on the configuration you have set, complete with a
   stream and dataset table; and the application will be ready to run.
  
   Note that errors can occur at the publishing phase that were not caught during
   validation; resolve those, if any, before continuing.
   
   The view changes to show the completed application:
   
   .. figure:: /_images/hydrator-gs-1-6-pipeline.png
     :figwidth: 100%
     :width: 6in
     :align: center
     :class: bordered-image
  
     **Cask Hydrator:** Demo pipeline

   Though this pipeline view is not editable, clicking the icons will bring up the same dialogs
   as before, showing which values have been configured for each stage of the pipeline.
   
#. The pipeline view has controls for launching the application and managing the physical
   application; important buttons to note are the *Run* (on the left) and the *View in
   CDAP* (on the right):

   .. figure:: /_images/hydrator-gs-1-8-pipeline-annotated.png
     :figwidth: 100%
     :width: 6in
     :align: center
     :class: bordered-image
  
     **Cask Hydrator:** Control Buttons, pipeline view

#. Before we can run the pipeline, we need to put data into the stream for the application to
   act on. Navigate to the stream by clicking on the *View in CDAP* button, then the
   *Datasets* button, and then the *demoStream* button:
   
    .. figure:: /_images/hydrator-gs-1-7-stream.png
      :figwidth: 100%
      :width: 6in
      :align: center
      :class: bordered-image
   
      **CDAP demoStream:** Status page, with *Actions* menu for sending events
      
   We can send events to the stream by clicking the *Actions* menu, selecting *Send
   Events*, and then clicking *Upload* in the dialog that follows. Navigate on your drive
   to your CDAP home directory, and locate the file ``examples/resources/accesslog.txt``
   included in your CDAP SDK. This file contains 10,000 records in CLF format.

   Once the file has been uploaded successfully by CDAP, you should see the *Total Events*
   for the stream change by 10,000.

   Return to *Hydrator* by clicking your browser back-button.
  
#. You can now run the pipeline. Click the *Run* button, located in the upper-left. (No
   runtime arguments are required; you can click *Start Now* in the dialog that appears.):

   The pipeline should start running, as indicated by the green *Running* text indicating
   the status. 
   
   .. figure:: /_images/hydrator-gs-1-9-pipeline-running.png
     :figwidth: 100%
     :width: 6in
     :align: center
     :class: bordered-image
  
     **Cask Hydrator:** *Running* pipeline

#. The number of records processed will, in time, change from zero to 10,000.
   When the run completes, the status icon will change to *Completed*. A start time and
   duration should appear in the status panel:

   .. figure:: /_images/hydrator-gs-1-10-completed.png
     :figwidth: 100%
     :width: 6in
     :align: center
     :class: bordered-image
  
     **Cask Hydrator:** Completed run of *demoPipeline*
      
#. You can now check the results by looking at the contents of the *demoTable*. Rather
   than using the *View in CDAP* button, we'll use a faster method to find the dataset.
   Click on the icon representing the table, to bring up the table configuration. In the
   upper-right of the dialog is a *Jump button* that brings down a menu with two items on
   it. The first one takes you directly to the table in CDAP. (The second takes you to the
   table in :ref:`Cask Tracker <cask-tracker-index>`.)

   .. figure:: /_images/hydrator-gs-1-11-jump-button.png
     :figwidth: 100%
     :width: 6in
     :align: center
     :class: bordered-image
  
     **Cask Hydrator:** *Jump button* in the pipeline configuration dialog

   Navigate to *demoTable* dataset, and run a default explore query that selects the first
   five records, by:
   
   - clicking *Explore* and then
   - clicking *Execute SQL*:

   .. figure:: /_images/hydrator-gs-1-12-demotable.png
     :figwidth: 100%
     :width: 6in
     :align: center
     :class: bordered-image
  
     **Cask Hydrator:** Results of exploring the *demoTable*
         
   Here you can see that the log records have been successfully loaded into the stream,
   parsed by the log parser, and then saved as parsed records to the table. This data is
   now available for further analysis, such looking for unique records, sorting, etc.
      
This completes the *Getting Started* for Cask Hydrator. 
