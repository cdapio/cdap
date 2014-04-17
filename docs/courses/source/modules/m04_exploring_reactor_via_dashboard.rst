============================================
Exploring Reactor Applications via Dashboard
============================================

.. include:: ../_slide-fragments/continuuity_logo.rst

----

Module Objectives
=================

In this module, you will look at the basic elements of Continuuity Reactor and its Dashboard.

The Dashboard is composed of five sections:

- Overview
- Collect
- Process
- Store
- Query

----

Module Objectives (continued)
=============================

You will see how these elements are displayed in the Dashboard:

- Streams
- Flows
- DataSets
- Procedures
- MapReduce Jobs
- Workflows

----

What is a Reactor Application?
==============================

- Application
- Streams
- Flows
- DataSets
- Procedures
- MapReduce Jobs
- Workflows



----

Overview
========
.fx: center_title_slide

----

Dashboard
=========

The **Continuuity Reactor Dashboard** is available for deploying, querying and 
managing the Continuuity Reactor.

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_01_overview.png
   :width: 600px

Here is a screen-capture of the Dashboard running on an Enterprise Continuuity Reactor.

.. class:: notes


Presenter Notes
---------------

**Reactor** gives you this starting overview, showing which Applications (*Apps*) are 
currently installed,
and realtime graphs of *Collect*, *Process*, *Store* and *Query*.
Each statistic is per unit of time—events per second, bytes (or larger) per second, 
queries per second—and
are sampled and reported based on the sampling menu in the upper right.

The lower portion of the screen shows all the Apps along with their name, description, 
and what is happening with each:

- *Collect*, the number of Streams consumed by the Application;

- *Process*, the number of Flows created by the Application;

- *Store*, the number of DataStores used by the Application;

- *Query*, the number of Procedures in the Application; and

- *Busyness*, the percentage of time spent processing events by the Application.

----

Application
===========

An Application is a collection of:

- Streams
- DataSets
- Flows
- Procedures
- MapReduce Jobs
- Workflows


----

Collect
=======
.fx: center_title_slide

----

Streams
=======

**Streams** are the primary means for bringing data
from external systems into the Reactor in realtime.

The **Collect** pane of the **Dashboard** shows all the Streams collecting data and their 
details: name, storage, number of events and the arrival rate, with a graph showing 
arrivals based on the sampling rate menu setting.

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_03_collect.png
   :width: 600px

----

Streams
=======

Clicking on a Stream's name will take you to the Stream's pane:

The Stream pane shows:

- the details of the number of events per second currently in the Stream,
- the storage and a graph of events over the last sampling period, and 
- a list of all the Flows
  that are attached to the Stream, with processing rate and busyness for each Flow.

Clicking on a Flow name will take you to that Flow's pane.

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_21_stream.png
   :width: 600px

----

Process
=======
.fx: center_title_slide

----

Process
=======
The **Process** pane shows all the
Flows,
MapReduce and Workflows in the Reactor.

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_04_process.png
   :width: 600px

Presenter Notes
---------------
With their name and status (either *Running* or *Stopped*).
Each name links to the individual elements detail pane.
Graphs show statistics based on the sampling rate menu setting.

In the case of Flows, it shows the processing rate in events per second and busyness.
For MapReduce, it shows the mapping status and the reducing status.

----

MapReduce Jobs
==============

For a MapReduce, the Mapping and Reducing activity is shown, along with status and 
management controls for starting, stopping and configuration.

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_26_mapreduce.png
   :width: 600px

----

Workflows
=========

For a Workflow, the time until the next scheduled run is shown, along with status and 
management controls for starting, stopping and configuration.

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_25_workflow.png
   :width: 600px


----

Flows
=====

Each Flow has a management pane, which shows the status, log and history of a Flow.
It that shows all the Streams, and Flowlets, connections, and icons arranged in a 
directed acyclic graph or DAG.
Across the top are two realtime graphs of processing rate and busyness with current 
Flow status and management controls.

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_07_app_crawler_flow_rss.png
   :width: 600px

----

Flows
=====

The upper-right portion has a cluster of buttons:

- Status, Log and History buttons that switch you between the panes of the Flow presentation;

- Sampling menu;

- Current status (*Running* or *Paused*);

- Gear icon for runtime configuration settings; and

- Start and stop buttons for the Flow.

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_11_app_crawler_detail.png
   :width: 200px

----

Flows
=====

The gear icon brings up a dialog for setting the runtime configuration parameters
that have been built into the Flow:

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_23_app_crawler_detail_config.png
   :width: 400px

----

Flows: Directed Acyclic Graph
=============================

The directed acyclic graph (DAG) shows all the Streams and Flowlets:

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_24_app_crawler_detail_dag.png
   :width: 600px


----

Flows: Directed Acyclic Graph
=============================

A Stream icon shows the name of the Stream and the number of events processed in the 
current sampling period:

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_12_stream_icon.png
   :width: 200px


----

Flows: Directed Acyclic Graph
=============================

A Flowlet icon shows the name of the Flowlet, the number of events processed
in the current sampling period,
and—in a small circle in the upper right of the icon—the number of instances of that Flowlet:

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_13_flowlet_icon.png
   :width: 200px

----

Store
=====
.fx: center_title_slide

----

DataSets
========

For a DataSet, write rate (in both bytes and operations per second), 
read rate and total storage is shown,
along with a list of Flows attached to the
DataSet and their processing rate and busyness.

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_15_dataset.png
   :width: 600px

----

Query
=====
.fx: center_title_slide

----

Procedures
==========

For a Procedure, request statistics are shown, along with status and management controls 
for starting, stopping and configuration. The dialog box shown allows for the generation 
of 'ad-hoc' requests, where JSON string parameters are passed to the Procedure when 
calling its methods.

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_17_procedure_ranker.png
   :width: 600px

----

Module Objectives Review
========================

In this module, you have seen the basic elements of Continuuity Reactor and its Dashboard:

- Overview
- Collect
- Process
- Store
- Query

----

Module Objectives (continued)
=============================

You have seen how these elements are displayed in the Dashboard:

- Streams
- Flows
- DataSets
- Procedures
- MapReduce Jobs
- Workflows
