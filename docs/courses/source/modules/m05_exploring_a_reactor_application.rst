============================================
Exploring an Example Reactor Application
============================================

.. reST Editor: .. section-numbering::
.. reST Editor: .. contents::

.. rst2pdf: CutStart
.. landslide: theme ../_theme/slides-generation/
.. landslide: build ../../html/

.. include:: ../_slide-fragments/continuuity_logo_copyright.rst

.. |br| raw:: html

   <br />
.. rst2pdf: CutStop

.. rst2pdf: config ../../../developer-guide/source/_templates/pdf-config
.. rst2pdf: stylesheets ../../../developer-guide/source/_templates/pdf-stylesheet
.. rst2pdf: build ../../pdf/
.. rst2pdf: .. |br|  unicode:: U+0020 .. space

----

Module Objectives
=================

In this module, you will see the basic elements of a
Continuuity Reactor Application through its Dashboard

You'll also learn about the components of the Dashboard
and its operation

----

Continuuity Reactor Dashboard
=============================

The **Continuuity Reactor Dashboard** is for deploying applications, querying and 
managing the Continuuity Reactor

The Dashboard is composed of five sections:

- Overview
- Collect
- Process
- Store
- Query

----

Dashboard: Overview
===================

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_01_overview.png
   :width: 80%

*Dashboard running on an Enterprise Continuuity Reactor*

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

Components of a Reactor Application
====================================

- Application
- Streams
- Flows
- Flowlets
- MapReduce Jobs
- Workflows
- DataSets
- Procedures

Elements of applications are located in the different
sections of the Dashboard based on their function

----

**Collect:** Streams
====================

**Streams**: primary means for bringing data
from external systems into the Reactor in realtime

Dashboard **Collect** pane shows all the Streams collecting data and their 
details: name, storage, number of events and the arrival rate, with a graph showing 
arrivals based on the sampling rate menu setting

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_03_collect.png
   :width: 80%

----

**Collect:** Streams
====================

Clicking on a Stream's name takes you to the Stream's pane, showing:

- Number of events per second currently in the Stream
- Storage and a graph of events over the last sampling period
- List of all the Flows attached to the Stream with processing rate and busyness for each
- Clicking on a Flow name will take you to that Flow's pane

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_21_stream.png
   :width: 80%

----

**Process**
===========
The **Process** pane shows all the
Flows,
MapReduce Jobs and Workflows in the Reactor

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_04_process.png
   :width: 80%

----

**Process**
===========

- Each name links to the individual elements detail pane
- Each element is shown with name and status (either *Running* or *Stopped*)
- Graphs show statistics based on the sampling rate menu setting
- For Flows, shows the processing rate in events per second and busyness
- For MapReduce Jobs, shows the mapping status and the reducing status

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_04_process.png
   :width: 80%

----

**Process:** Flows
==================

- Each Flow has a management pane, with status, log and history
- Shows all Streams, Flowlets, connections, and icons arranged in a 
  directed acyclic graph or DAG
- Two realtime graphs of processing rate and busyness with current 
  Flow status and management controls

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_07_app_crawler_flow_rss.png
   :width: 80%

----

**Flows:** Management Cluster
=============================

The upper-right management cluster:

- Status, Log and History buttons that switch you between the panes of the Flow presentation
- Sampling menu
- Current status (*Running* or *Paused*)
- Gear icon for runtime configuration settings
- Start and stop buttons for the Flow

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_11_app_crawler_detail.png
   :width: 40%

----

**Flows:** Configuration Parameters
===================================

The gear icon brings up a dialog for setting the runtime configuration parameters
that have been built into the Flow:

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_23_app_crawler_detail_config.png
   :width: 40%

----

**Flows:** Directed Acyclic Graph
=================================

The directed acyclic graph (DAG) shows all the Streams and Flowlets:

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_24_app_crawler_detail_dag.png
   :width: 60%

----

**Flows:** Directed Acyclic Graph
=================================

A Stream icon shows:

- Stream name
- Number of events processed in the current sampling period

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_12_stream_icon.png
   :width: 40%

----

**Flows:** Directed Acyclic Graph
=================================

A Flowlet icon shows:

- Flowlet name
- Number of events processed in the current sampling period
- Number of instances of that Flowlet (small circle, upper right of the icon)

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_13_flowlet_icon.png
   :width: 40%

----

**Process:** MapReduce Jobs
===========================

Mapping and Reducing activity is shown, along with status and 
management controls for starting, stopping and configuration

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_26_mapreduce.png
   :width: 80%

----

**Process:** Workflows
======================

Time until the next scheduled run is shown, along with status and 
management controls for starting, stopping and configuration

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_25_workflow.png
   :width: 80%

----

**Store:** DataSets
===================

- Write rate (in both bytes and operations per second) 
- Read rate and total storage
- List of Flows attached to the DataSet with their processing rate and busyness

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_15_dataset.png
   :width: 80%

----

**Query:** Procedures
=====================

- Request statistics
- Status and management controls for starting, stopping and configuration
 
The dialog box shown allows for the generation of 'ad-hoc' requests, 
where JSON string parameters are passed to the Procedure when calling its methods:

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_17_procedure_ranker.png
   :width: 80%

----

Module Summary
==============

In this module, you now know:

- The basic elements of a Continuuity Reactor Application
- Location of these basic elements in the Continuuity Reactor Dashboard
- Functional areas of the Dashboard

----

Module Completed
================

`Chapter Index <return.html#m05>`__
