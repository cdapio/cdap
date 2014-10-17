.. :author: Cask Data, Inc.
   :copyright: Copyright © 2014 Cask Data, Inc.

==================================================================
CDAP Console
==================================================================

.. _console:

Overview
--------

The **CDAP Console** is available for deploying, querying and managing the Cask Data Application Platform in all modes of CDAP except an 
`In-memory CDAP <#in-memory-data-application-platform>`__.

.. image:: _images/console/console_01_overview.png
   :width: 600px

Here is a screen-capture of the CDAP Console running on a Distributed CDAP.

Down the left sidebar, underneath the **Cask** logo, is the CDAP mode identifier (in this case, *Distributed CDAP*), followed by four buttons:
*Application*, `Process`_, `Store`_ and `Query`_. These buttons gives you access to CDAP Console facilities for managing each of these CDAP elements.

In the far upper-right are two buttons: the `Metrics <#metrics-explorer>`__ and
`Services <#services-explorer>`__ buttons, which take you to their respective explorers.

.. _sampling-menu:

In the upper right portion of the display is a menu and, in the Distributed version of 
CDAP, two buttons (*CDAP* and *Resources*).
The menu is the **Sampling Menu**, which appears on almost every pane of the
CDAP Console. 

The menu determines how much data is sampled in the presentation shown on the CDAP Console in
realtime:

.. image:: _images/console/console_10_app_crawler_detail_menu.png
   :width: 200px

By default, the sampling menu is set at "Last 1 Minute", indicating that the graphs are showing
the last one minute of activity. Be aware that changing the menu to a longer period (such as "Last 
1 Hour") can adversely affect the performance of the CDAP Instance and should only be used for short sessions before returning the setting to the default "Last 1 Minute".

This starting overview, showing which Applications (*Apps*) are currently
installed, and realtime graphs of *Collect*, *Process*, *Store*, and *Query*.
Each statistic is per unit of time—events per second, bytes (or larger) per second, queries per second—and
are sampled and reported based on the sampling menu in the upper right. (In Distributed CDAP, this starting overview can be reached by the **CDAP** button.)

The lower portion of the screen shows all the Apps along with their name, description, and what is happening with each:

- *Collect*, the number of Streams consumed by the Application;

- *Process*, the number of Flows created by the Application;

- *Store*, the number of DataStores used by the Application;

- *Query*, the number of Procedures in the Application; and

- *Busyness*, the percentage of time spent processing events by the Application.

.. _busyness:

Busyness—the percentage of time spent processing events—is a concept that is used extensively in the CDAP Console.

Clicking on the name of an Application will take you to the `App's pane <#application>`__, with details about the application.

:Note: Because of the interconnections in the CDAP Console, you can arrive at the same pane by different paths.
       Remember that the left pane buttons will always take you back to the initial summary panes.

The **Resources** button (available in Distributed CDAP) gives a look at what is being used by the CDAP:

.. image:: _images/console/console_02_overview_resources.png
   :width: 600px

Once again, the top half shows four different elements, all in realtime:
*AppFabric* consumption (in Yarn containers),
*Processors* used (in the number of cores),
*Memory* consumption (total bytes available and used memory), and
*DataFabric* storage (total bytes available and used disk space).

*Containers* refers to the number of Yarn containers; for example, each Flowlet instance uses a single container.

Statistics and graphs are sampled based on the setting of the sampling menu in the upper-right.

The lower half shows the list of deployed applications, their descriptions, along with each app's container, core and memory use in realtime.

The triangles to the left of each application turn to reveal the hierarchy of resources being used by each application's Flows and Flowlets. You can use this tree to drill down into any part of the CDAP.

The footer of each pane gives—below the *Cask Data, Inc.* copyright—five buttons
and the version of the CDAP that you are using.

.. _reset:

The five buttons provide access to the `terms of use <http://cask.co/terms>`__,
the `privacy policy <http://cask.co/privacy>`__,
contacting `Cask <http://cask.co/company/#company-contact>`__,
contacting Cask support, and *Reset*, for resetting the CDAP.

*Reset* deletes all data and applications from the
CDAP, is irreversible, and returns the CDAP to an original state. The button is only visible and
available if the CDAP has been started with the system property ``enable.unrecoverable.reset`` as ``true``. 

Metrics Explorer
----------------

In the top portion of the `Overview image <#console>`__ you can see the **Metrics** button, which takes you to the *Metrics Explorer:*

.. image:: _images/console/console_18_metrics_explorer1.png
   :width: 600px

Here you can monitor a variety of different statistics for elements of the CDAP.
You add a metric by clicking the *Add* button; it will give you a dialog
where you can specify an element and then pick from a list of appropriate metrics.

.. image:: _images/console/console_20_metrics_explorer3.png
   :width: 200px

As with other CDAP Console realtime graphs, you specify the sampling rate through a pop-down menu in the
upper-right. You can *Pause* the sampling to prevent excessive load on the CDAP.

If you move your mouse over the graph, you will get detailed information about the statistics presented:

.. image:: _images/console/console_19_metrics_explorer2.png
   :width: 600px

System Services Explorer
------------------------
In the top portion of the `Overview image <#console>`__, to the right of the **Metrics** button is the
**Services** button, which takes you to the *Services Explorer:*


.. image:: _images/console/console_31_services_explorer.png
   :width: 600px

Here you can monitor a variety of different System Services of the CDAP. For each service name, status
is given, if logs are available (and link to them if so), the number of instances requested and
provisioned.

.. _Collect:

Collect
-------
.. image:: _images/console/console_03_collect.png
   :width: 600px

The **Collect** pane shows all the Streams collecting data and their details: name, storage, number of events and the arrival rate, with a graph showing arrivals based on the sampling rate menu setting.

.. _Stream:

Clicking on a Stream's name will take you to the Stream's pane:

.. image:: _images/console/console_21_stream.png
   :width: 600px

The Stream pane shows the details of the number of events per second currently in the Stream,
the storage and a graph of events over the last sampling period, and a list of all the Flows
that are attached to the Stream, with processing rate and `busyness`_ for each Flow.
Clicking on a Flow name will take you to that `Flow's pane <#flow>`__.


.. _Process:

Process
-------

.. image:: _images/console/console_04_process.png
   :width: 600px

The **Process** pane shows all the
`Flows <#flow>`__,
`MapReduce <#mapreduce>`__ and
`Workflows <#workflow>`__ in the CDAP
with their name and status (either *Running* or *Stopped*).
Each name links to the individual elements detail pane.
Graphs show statistics based on the sampling rate menu setting.

In the case of Flows, it shows the processing rate in events per second and `busyness`_. For MapReduce, it shows the mapping status and the reducing status.


.. _Store:

Store
-----

.. image:: _images/console/console_05_store.png
   :width: 600px

The **Store** pane shows all the Datasets currently specified in the CDAP, along with their name
(a link to the detail pane for the Dataset), type (the Java class), storage in use,
a realtime write-rate graph and the current write rate (bytes per second). It has button that accesses the
`Dataset Explorer`_.


Dataset Explorer
................
From within the `Store`_ pane you can access the Dataset Explorer, which allows for SQL-like
queries of the datasets' underlying Hive tables. Details on the requirements for formulating and
performing these queries can be found in the Developer Guide :ref:`data-explore`.

Using the information supplied for each Hive table (schema, keys, properties) you can generate a
SQL-like query and then execute it.

.. image:: _images/console/console_33_query_explorer.png
   :width: 600px

When the query has completed, it will be listed on the *Results* pane of the Explorer. The results
can either be viewed directly or downloaded to your computer.

.. image:: _images/console/console_35_query_explorer.png
   :width: 600px

Double-clicking on the results will reveal them in the browser:

.. image:: _images/console/console_37_query_explorer.png
   :width: 600px

If no results are available, the "Download" icon will be greyed-out and hovering over it will display a
message "Results Not Available".

.. image:: _images/console/console_36_query_explorer.png
   :width: 600px


.. _Query:

Query
-----

.. image:: _images/console/console_06_query.png
   :width: 600px

The **Query** pane shows all the Procedures currently specified in the CDAP, along with their name
(a link to the detail pane for the Procedure), status and realtime graphs
of their request and error rates.


.. _application:

Application
-----------

.. image:: _images/console/console_14_app_crawler.png
   :width: 600px

The Application pane shows details for an individual application deployed in the CDAP:

- **Summary graphs:** across the top, left to right, a summary of events per second processed,
  `busyness`_ and storage;

- **Collect:** Streams, with name (a link to details) and summary statistics;

- **Process:** Flows, with name (a link to details), summary statistics,
  and a management button to start and stop all the Flows associated with this app;

- **Store:** Datasets defined by this Application, with name (a link to details)
  and summary statistics; and

- **Query:** Procedures, with name (a link to details) and summary statistics,
  and a management button to start and stop all the Procedures associated with this app;

- **Service:** Services, with name (a link to details) and number of components,
  and a management button to start and stop all the Services associated with this app.

Deleting an Application
.......................

The button in the upper right of the pane allows you to delete the current Application:

.. image:: _images/console/console_22_app_crawler_detail_delete.png
   :width: 200px

However, before an Application can be deleted, all Process—Flows and MapReduce Jobs—and Queries (Procedures), must be stopped.
An error message will be given if you attempt to delete an Application with running components.

Note that Streams and Datasets, even though they are specified and created at the time of deployment of the Application,
are persistent and are not deleted when an Application is deleted.

To delete these, the CDAP needs to be reset using the `Reset button <#reset>`__ located at the bottom of each pane.


.. _flow:

Flow
----

Each Flow has a management pane, which shows the status, log and history of a Flow.


Flow Status
...........
Start by looking at the status of a Flow:

.. image:: _images/console/console_07_app_crawler_flow_rss.png
   :width: 600px

It shows all of the Streams and Flowlets of the Flow with their connections and icons arranged in a
directed acyclic graph or DAG.

Across the top are two realtime graphs of processing rate and `busyness`_ with
current Flow status and management controls.

.. image:: _images/console/console_11_app_crawler_detail.png
   :width: 200px

The upper-right portion has a cluster of buttons:

- Status, Log and History buttons that switch you between the panes of the Flow presentation;

- `Sampling menu <#sampling-menu>`__;

- Current status (*Running* or *Paused*);

- Gear icon for runtime configuration settings; and

- Start and stop buttons for the Flow.

The gear icon brings up a dialog for setting the runtime configuration parameters
that have been built into the Flow:

.. image:: _images/console/console_23_app_crawler_detail_config.png
   :width: 400px

The directed acyclic graph (DAG) shows all the Streams and Flowlets:

.. image:: _images/console/console_24_app_crawler_detail_dag.png
   :width: 600px

A Stream icon shows the name of the Stream and the number of events processed in the current sampling period:

.. image:: _images/console/console_12_stream_icon.png
   :width: 200px

A Flowlet icon shows the name of the Flowlet, the number of events processed
in the current sampling period,
and—in a small circle in the upper right of the icon—the number of instances of that Flowlet:

.. image:: _images/console/console_13_flowlet_icon.png
   :width: 200px


DAG Icon Dialogs
................

Clicking on an icon in the DAG brings up the icon's dialog. This dialog contains numerous buttons and panes,
and allows you to traverse the DAG completely by selecting appropriate inputs and outputs.

.. image:: _images/console/console_27_dag1.png
   :width: 400px

Here we have clicked on a Flowlet named *counter*, and are seeing the first
(*Inputs*) of three panes in this dialog. On the left is a list of inputs to the Flowlet,
in this case a single input Stream named *parser*, and realtime statistics for the flowlet.

Clicking the name *parser* would take you—without leaving the dialog—backwards on the path
of the DAG, and allow you to traverse towards the start of the path.

If you go all the way to the beginning of the path, you will reach a Stream, and the dialog will change:

.. image:: _images/console/console_30_dag4.png
   :width: 400px

Here, you can inject an Event into the Stream simply by typing and pressing the *Inject* button.
(Notice that once you have reached a Stream, there is no way to leave on the DAG. There
is no list of consumers of the Stream.)

Returning to the `original dialog <#dag-icon-dialogs>`__, clicking the "Processed" button in the center takes you to the second pane of the dialog.

.. image:: _images/console/console_28_dag2.png
   :width: 400px

Here are realtime statistics for the processing rate, `busyness`_, data operations and errors.

Clicking the "Outputs" button on the right takes you to the third pane of the dialog.

.. image:: _images/console/console_29_dag3.png
   :width: 400px

On the right are all the output connections of the Flowlet, if any, and clicking any of
the names would take you to that Flowlet’s input pane, allowing you to traverse the graph
in the direction of data flow. The realtime statistics for the outbound events are shown.

In the upper right portion of this dialog you can set the requested number of instances.
The current number of instances is shown for reference.


.. _log-explorer:

Flow Log Explorer
.................

The Flow Log Explorer pane shows a sample from the logs, with filters for a standard set of filters: *Info*, *Warning*, *Error*, *Debug*, and *Other:*

.. image:: _images/console/console_08_app_crawler_flow_rss_log.png
   :width: 600px

Flow History
................

The Flow History pane shows started and ended events for the Flow and the results:

.. image:: _images/console/console_09b_app_crawler_flow_rss_history.png
   :width: 600px


MapReduce
---------
For a MapReduce, the Mapping and Reducing activity is shown, along with status and management controls for starting,
stopping and configuration. Buttons for logs and history, similar to those for
`Flows <#flow-history>`__ and `Workflows <#workflow>`__, are also available:


.. image:: _images/console/console_26_mapreduce.png
   :width: 600px


Spark
-----
For a Spark program, the status is shown along with the management controls for starting,
stopping and configuration. Buttons for logs and history, similar to those for
`Flows <#flow-history>`__ and `Workflows <#workflow>`__, are also available:

.. image:: _images/console/console_26a_spark.png
   :width: 600px


Workflow
--------
For a Workflow, the time until the next scheduled run is shown, along with status and management controls for starting, stopping and configuration.

.. image:: _images/console/console_25_workflow.png
   :width: 600px


Workflow History
................
The Workflow History pane shows started and ended events for the Workflow and the results:

.. image:: _images/console/console_09_app_crawler_flow_rss_history.png
   :width: 600px

Dataset
-------
For a Dataset, write rate (in both bytes and operations per second), read rate and total storage is shown
along with a list of Flows attached to the Dataset, their processing rate, and `busyness`_.

.. image:: _images/console/console_15_dataset.png
   :width: 600px

Procedure
---------
For a Procedure, request statistics are shown, along with status and management controls for starting, stopping and configuration. The dialog box shown allows for the sending of requests to Procedures, where
JSON string parameters are passed to the Procedure when calling its methods.

For details of making requests and using Procedures, including configuring the parameters and calling
methods, see the :ref:`restful-api`.

In a fashion similar to the `Flow Log Explorer`_, you can examine the logs associated with each Procedure.


.. image:: _images/console/console_17_procedure_ranker.png
   :width: 600px

Service
-------
Each Application can access and use user-defined Services. From an individual Application's panel
you access its Services panel.

For a Service, components of the Service are shown, along with status and management controls for starting,
stopping and configuration. The current number of instances requested and active are shown for
each component.

For details of making and using Custom Services, see the Developer Guide section on :ref:`user-services`.

.. image:: _images/console/console_32_custom_service.png
   :width: 600px

