.. meta::
    :author: Cask Data, Inc 
    :description: Release notes for the Cask Data Application Platform
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

:hide-nav: true
:orphan:

.. _overview_release-notes:

.. index::
   single: Release Notes

.. _release-notes:

============================================
Cask Data Application Platform Release Notes
============================================

.. contents::
   :local:
   :class: faq
   :backlinks: none
   :depth: 2


`Release 3.0.6 <http://docs.cask.co/cdap/3.0.6/index.html>`__
=============================================================

Bug Fixes
---------

- Fixed a problem with faulty and unused metrics regarding CDAP file resource usage.
  (`CDAP-3563 <https://issues.cask.co/browse/CDAP-3563>`__).

- Fixed a problem where a Hadoop filesystem object was being instantiated before the
  Kerberos keytab login was completed, leading to CDAP processes failing after the initial
  ticket expired (`CDAP-3660 <https://issues.cask.co/browse/CDAP-3660>`__).

- Fixed a problem where not all dependent services were being stopped when a system service was stopped 
  (`CDAP-3861 <https://issues.cask.co/browse/CDAP-3861>`__).

- Fixed a problem with dataset performance degradation due to the making of 
  multiple remote calls for each "get dataset" request 
  (`CDAP-3902 <https://issues.cask.co/browse/CDAP-3902>`__).

- Fixed a problem with not closing the socket connection to the transaction service correctly, 
  leading to a leakage of file descriptors 
  (`CDAP-4067 <https://issues.cask.co/browse/CDAP-4067>`__).

- Fixed a problem with the CDAP Router being tied up in an infinite recursion 
  (`CDAP-4092 <https://issues.cask.co/browse/CDAP-4092>`__).

- Fixed problems with the installation documentation 
  (`CDAP-4093 <https://issues.cask.co/browse/CDAP-4093>`__).

- Fixed an issue with the incorrect labelling, after failover, of programs that had been running prior to failover 
  (`CDAP-4119 <https://issues.cask.co/browse/CDAP-4119>`__).

New Features
------------

- Updated the version of `Tephra <http://tephra.io>`__ used in CDAP to v0.4.4
  (`CDAP-4181 <https://issues.cask.co/browse/CDAP-4181>`__).

- Provided a Nagios plugin for monitoring the health of CDAP System Services 
  (`CDAP-3967 <https://issues.cask.co/browse/CDAP-3967>`__).

- Added the capability of retrieving live-info for the CDAP AppFabric system service 
  (`CDAP-4133 <https://issues.cask.co/browse/CDAP-4133>`__).


`Release 3.0.5 <http://docs.cask.co/cdap/3.0.5/index.html>`__
=============================================================

Bug Fix
-------

- Fixed a bug that prevents streams events that are already processed from being re-processed in flows. 
  (`CDAP-3458 <https://issues.cask.co/browse/CDAP-3458>`__).
  

`Release 3.0.4 <http://docs.cask.co/cdap/3.0.4/index.html>`__
=============================================================

Bug Fixes
---------

- Fixed a problem with a readless increment co-processor throwing an IllegalStateException during flush
  (`CDAP-3449 <https://issues.cask.co/browse/CDAP-3449>`__).
  
- The HBaseQueueDebugger now shows the minimum queue event transaction write pointer both per-queue and for all queues
  (`CDAP-3419 <https://issues.cask.co/browse/CDAP-3419>`__).
 
- Fixed a problem with the logback-container.xml not being copied into CDAP-Master services
  (`CDAP-3362 <https://issues.cask.co/browse/CDAP-3362>`__).
  

`Release 3.0.3 <http://docs.cask.co/cdap/3.0.3/index.html>`__
=============================================================

Bug Fix
-------

- Fixed a Bower dependency error in the CDAP UI
  (`CDAP-3010 <https://issues.cask.co/browse/CDAP-3010>`__).


`Release 3.0.2 <http://docs.cask.co/cdap/3.0.2/index.html>`__
=============================================================

Bug Fixes
---------

- Fixed problems with the dataset upgrade tool
  (`CDAP-2962 <https://issues.cask.co/browse/CDAP-2962>`__, 
  `CDAP-2897 <https://issues.cask.co/browse/CDAP-2897>`__).


`Release 3.0.1 <http://docs.cask.co/cdap/3.0.1/index.html>`__
=============================================================

New Features
------------

- In the CDAP UI, mandatory parameters for Application Template creation are marked with
  asterisks, and if a user tries to create a template without one of those parameters, the
  missing parameter is highlighted
  (`CDAP-2499 <https://issues.cask.co/browse/CDAP-2499>`__).


Improvements
------------

**Tools**

- Added a tool (`HBaseQueueDebugger 
  <https://github.com/caskdata/cdap/blob/release/3.0/cdap-master/src/main/java/co/cask/cdap/data/tools/HBaseQueueDebugger.java>`__)
  that counts consumed and unconsumed entries in a flowlet queue
  (`CDAP-2105 <https://issues.cask.co/browse/CDAP-2105>`__).

**CDAP UI**

- The currently executing node of a workflow is now highlighted in the CDAP UI
  (`CDAP-2615 <https://issues.cask.co/browse/CDAP-2615>`__).
  
- The list of datasets and the run histories in the CDAP UI are now paginated 
  (`CDAP-2626, CDAP-2627 <https://issues.cask.co/browse/CDAP-2626>`__).
  
- Added improvements to the CDAP UI when creating Application Templates
  (`CDAP-2601, CDAP-2602, CDAP-2603, CDAP-2605, CDAP-2606, CDAP-2607, CDAP-2610
  <https://issues.cask.co/browse/CDAP-2601>`__).
  
- Improved the error messages returned when there are problems creating Application 
  Templates in the CDAP UI
  (`CDAP-2597 <https://issues.cask.co/browse/CDAP-2597>`__).
  
**CDAP SDK VM**

- Added the Apache Flume agent flume-ng to the CDAP SDK VM
  (`CDAP-2612 <https://issues.cask.co/browse/CDAP-2612>`__).

- Added the ability to copy and paste to the CDAP SDK VM
  (`CDAP-2611 <https://issues.cask.co/browse/CDAP-2611>`__).

- Pre-downloaded the example dependencies into the CDAP SDK VM to speed building of the 
  CDAP examples
  (`CDAP-2613 <https://issues.cask.co/browse/CDAP-2613>`__).


Bug Fixes
---------

**General**

- Fixed a problem with the HBase store and flows with multiple queues, where one queue name
  is a prefix of another queue name
  (`CDAP-1996 <https://issues.cask.co/browse/CDAP-1996>`__).
  
- Fixed a problem with namespaces with underscores in the name crashing the Hadoop HBase 
  region servers
  (`CDAP-2110 <https://issues.cask.co/browse/CDAP-2110>`__).
  
- Removed the requirement to specify the JDBC driver class property twice in the adaptor
  configuration for Database Sources and Sinks
  (`CDAP-2453 <https://issues.cask.co/browse/CDAP-2453>`__).

- Fixed a problem in CDAP Distributed where the status of running program always returns 
  as "STOPPED" when the CDAP Master is restarted
  (`CDAP-2489 <https://issues.cask.co/browse/CDAP-2489>`__).
  
- Fixed a problem with invalid RunRecords for Spark and MapReduce programs that are run as 
  part of a Workflow (`CDAP-2490 <https://issues.cask.co/browse/CDAP-2490>`__).
  
- Fixed a problem with the CDAP Master not being HA (highly available) when a leadership 
  change happens
  (`CDAP-2495 <https://issues.cask.co/browse/CDAP-2495>`__).
  
- Fixed a problem with upgrading of queues with the UpgradeTool
  (`CDAP-2502 <https://issues.cask.co/browse/CDAP-2502>`__).
  
- Fixed a problem with ObjectMappedTables not deleting missing fields when updating a row
  (`CDAP-2523, CDAP-2524 <https://issues.cask.co/browse/CDAP-2523>`__).
  
- Fixed a problem with a stream not being created properly when deploying an application
  after the default namespace was deleted
  (`CDAP-2537 <https://issues.cask.co/browse/CDAP-2537>`__).
  
- Fixed a problem with the Applicaton Template Kafka Source not using the persisted offset
  when the Adapter is restarted
  (`CDAP-2547 <https://issues.cask.co/browse/CDAP-2547>`__).
  
- A problem with CDAP using its own transaction snapshot codec, leading to huge snapshot
  files and OutOfMemory exceptions, and transaction snapshots that can't be read using
  Tephra's tools, has been resolved by replacing the codec with Tephra's SnapshotCodecV3
  (`CDAP-2563, CDAP-2946, TEPHRA-101 <https://issues.cask.co/browse/CDAP-2563>`__).
  
- Fixed a problem with CDAP Master not being resilient in the handling of Zookeeper 
  exceptions
  (`CDAP-2569 <https://issues.cask.co/browse/CDAP-2569>`__).
  
- Fixed a problem with RunRecords not being cleaned up correctly after certain exceptions
  (`CDAP-2584 <https://issues.cask.co/browse/CDAP-2584>`__).
  
- Fixed a problem with the CDAP Maven archetype having an incorrect CDAP version in it
  (`CDAP-2634 <https://issues.cask.co/browse/CDAP-2634>`__).
  
- Fixed a problem with the description of the TwitterSource not describing its output
  (`CDAP-2648 <https://issues.cask.co/browse/CDAP-2648>`__).
  
- Fixed a problem with the Twitter Source not handling missing fields correctly and as a
  consequence producing tweets (with errors) that were then not stored on disk
  (`CDAP-2653 <https://issues.cask.co/browse/CDAP-2653>`__).
  
- Fixed a problem with the TwitterSource not calculating the time of tweet correctly
  (`CDAP-2656 <https://issues.cask.co/browse/CDAP-2656>`__).
  
- Fixed a problem with the JMS Real-time Source failing to load required plugin sources
  (`CDAP-2661 <https://issues.cask.co/browse/CDAP-2661>`__).

- Fixed a problem with executing Hive queries on a distributed CDAP due to a failure to 
  load Grok classes
  (`CDAP-2678 <https://issues.cask.co/browse/CDAP-2678>`__).
  
- Fixed a problem with CDAP Program jars not being cleaned up from the temporary directory
  (`CDAP-2698 <https://issues.cask.co/browse/CDAP-2698>`__).
  
- Fixed a problem with ProjectionTransforms not handling input data fields with null 
  values correctly
  (`CDAP-2719 <https://issues.cask.co/browse/CDAP-2719>`__).

- Fixed a problem with the CDAP SDK running out of memory when MapReduce jobs are run repeatedly
  (`CDAP-2743 <https://issues.cask.co/browse/CDAP-2743>`__).

- Fixed a problem with not using CDAP RunIDs in the in-memory version of the CDAP SDK
  (`CDAP-2769 <https://issues.cask.co/browse/CDAP-2769>`__).

**CDAP CLI**

- Fixed a problem with the CDAP CLI not printing an error if it is unable to connect to a 
  CDAP instance
  (`CDAP-2529 <https://issues.cask.co/browse/CDAP-2529>`__).
  
- Fixed a problem with extra whitespace in commands entered into the CDAP CLI causing errors
  (`CDAP-2538 <https://issues.cask.co/browse/CDAP-2538>`__).
  
**CDAP SDK Standalone**

- Updated the messages displayed when starting the CDAP Standalone SDK as to components 
  and the JVM required (`CDAP-2445 <https://issues.cask.co/browse/CDAP-2445>`__).
  
- Fixed a problem with the creation of the default namespace upon starting the CDAP SDK
  (`CDAP-2587 <https://issues.cask.co/browse/CDAP-2587>`__).
  
**CDAP SDK VM**

- Fixed a problem with using the default namespace on the CDAP SDK Virtual Machine Image
  (`CDAP-2500 <https://issues.cask.co/browse/CDAP-2500>`__).

- Fixed a problem with the VirtualBox VM retaining a MAC address obtained from the build host
  (`CDAP-2640 <https://issues.cask.co/browse/CDAP-2640>`__).
  
**CDAP UI**

- Fixed a problem with incorrect flow metrics showing in the CDAP UI
  (`CDAP-2494 <https://issues.cask.co/browse/CDAP-2494>`__).
  
- Fixed a problem in the CDAP UI with the properties in the Projection Transform being 
  displayed inconsistently
  (`CDAP-2525 <https://issues.cask.co/browse/CDAP-2525>`__).
  
- Fixed a problem in the CDAP UI not automatically updating the number of flowlet instances
  (`CDAP-2534 <https://issues.cask.co/browse/CDAP-2534>`__).
  
- Fixed a problem in the CDAP UI with a window resize preventing clicking of the Adapter 
  Template drop down menu
  (`CDAP-2573 <https://issues.cask.co/browse/CDAP-2573>`__).
  
- Fixed a problem with the CDAP UI not performing validation of mandatory parameters 
  before the creation of an adapter
  (`CDAP-2575 <https://issues.cask.co/browse/CDAP-2575>`__).
  
- Fixed a problem with an incorrect version of CDAP being shown in the CDAP UI
  (`CDAP-2586 <https://issues.cask.co/browse/CDAP-2586>`__).

- Reduced the number of clicks required to navigate and perform actions within the CDAP UI
  (`CDAP-2622, CDAP-2625 <https://issues.cask.co/browse/CDAP-2622>`__).
  
- Fixed a problem with an additional forward-slash character in the URL causing a "page 
  not found error" in the CDAP UI
  (`CDAP-2624 <https://issues.cask.co/browse/CDAP-2624>`__).
  
- Fixed a problem with the error dropdown of the CDAP UI not scrolling when it has a 
  large number of errors
  (`CDAP-2633 <https://issues.cask.co/browse/CDAP-2633>`__).
  
- Fixed a problem in the CDAP UI with the Twitter Source's consumer key secret not being 
  treated as a password field
  (`CDAP-2649 <https://issues.cask.co/browse/CDAP-2649>`__).
  
- Fixed a problem with the CDAP UI attempting to create an adapter without a name
  (`CDAP-2652 <https://issues.cask.co/browse/CDAP-2652>`__).
  
- Fixed a problem with the CDAP UI not being able to find the ETL plugin templates on
  distributed CDAP
  (`CDAP-2655 <https://issues.cask.co/browse/CDAP-2655>`__).
  
- Fixed a problem with the CDAP UI's System Dashboard chart having a y-axis starting at "-200"
  (`CDAP-2699 <https://issues.cask.co/browse/CDAP-2699>`__).

- Fixed a problem with the rendering of stack trace logs in the CDAP UI
  (`CDAP-2745 <https://issues.cask.co/browse/CDAP-2745>`__).
  
- Fixed a problem with the CDAP UI not working with secure CDAP instances, either clusters 
  or standalone
  (`CDAP-2770 <https://issues.cask.co/browse/CDAP-2770>`__).
  
- Fixed a problem with the coloring of completed runs of Workflow DAGs in the CDAP UI
  (`CDAP-2781 <https://issues.cask.co/browse/CDAP-2781>`__).
  
**Documentation**

- Fixed errors with the documentation examples of the ETL Plugins
  (`CDAP-2503 <https://issues.cask.co/browse/CDAP-2503>`__).
  
- Documented the licenses of all shipped CDAP UI components
  (`CDAP-2582 <https://issues.cask.co/browse/CDAP-2582>`__).

- Corrected issues with the building of Javadocs used on the website and removed Javadocs
  previously included in the SDK
  (`CDAP-2730 <https://issues.cask.co/browse/CDAP-2730>`__).
  
- Added a recommended version (v.12.0) of Node.js to the documentation
  (`CDAP-2762 <https://issues.cask.co/browse/CDAP-2762>`__).
  

API Changes
-----------


Deprecated and Removed Features
-------------------------------


.. _known-issues-301:

Known Issues
------------

- The application in the `cdap-kafka-ingest-guide 
  <https://github.com/cdap-guides/cdap-kafka-ingest-guide/tree/release/cdap-3.0-compatible>`__ 
  does not run on Ubuntu 14.x and CDAP 3.0.x
  (`CDAP-2632, CDAP-2749 <https://issues.cask.co/browse/CDAP-2632>`__).

- Metrics for :ref:`TimePartitionedFileSets <datasets-timepartitioned-fileset>` can show 
  zero values even if there is data present
  (`CDAP-2721 <https://issues.cask.co/browse/CDAP-2721>`__).
  
- In the CDAP UI: many buttons will remain in focus after being clicked, even if they
  should not retain focus
  (`CDAP-2785 <https://issues.cask.co/browse/CDAP-2785>`__).
  
- When the CDAP-Master dies, the CDAP UI does not repsond appropriately, and instead of 
  waiting for routing to the secondary master to begin, it loses its connection
  (`CDAP-2830 <https://issues.cask.co/browse/CDAP-2830>`__).

- A workflow that is scheduled by time will not be run between the failure of the primary
  master and the time that the secondary takes over. This scheduled run will not be
  triggered at all. There is no warnings or messages about the missed run of the
  workflow. (`CDAP-2831 <https://issues.cask.co/browse/CDAP-2831>`__)


- See also the *Known Issues* of `version 3.0.0 <#known-issues-300>`_\ .



`Release 3.0.0 <http://docs.cask.co/cdap/3.0.0/index.html>`__
=============================================================

New Features
------------

- Support for Application Templates has been added (`CDAP-1753 <https://issues.cask.co/browse/CDAP-1753>`__).

- Built-in ETL Application Templates and Plugins have been added (`CDAP-1767 <https://issues.cask.co/browse/CDAP-1767>`__).

- New :ref:`CDAP UI <cdap-ui>`, supports creating ETL applications directly in the web UI.
  See section below (:ref:`New User Interface <new-user-interface-300>`) for details.

- Workflow logs can now be retrieved using the :ref:`CDP HTTP Logging RESTful API 
  <http-restful-api-logging>` (`CDAP-1089 <https://issues.cask.co/browse/CDAP-1089>`__).
  
- Support has been added for suspending and resuming of a Workflow (`CDAP-1610
  <https://issues.cask.co/browse/CDAP-1610>`__).
  
- Condition nodes in a Workflow now allow branching based on a boolean predicate
  (`CDAP-1928 <https://issues.cask.co/browse/CDAP-1928>`__).
  
- Condition nodes in a Workflow now allow passing the Hadoop counters from a MapReduce
  program to following Condition nodes in the Workflow (`CDAP-1611
  <https://issues.cask.co/browse/CDAP-1611>`__).
  
- Logs can now be fetched based on the ``run-id`` (`CDAP-1582
  <https://issues.cask.co/browse/CDAP-1582>`__).
  
- CDAP Tables are :ref:`now explorable <table-exploration>` (`CDAP-946
  <https://issues.cask.co/browse/CDAP-946>`__).

- The :ref:`CDAP CLI <cli>` supports the new :ref:`Application Template and Adapters APIs 
  <apptemplates-index>`. (`CDAP-1773 <https://issues.cask.co/browse/CDAP-1773>`__).
  
- The :ref:`CDAP CLI <cli>` startup options have been changed to accommodate a new option
  of executing a file containing a series of CLI commands, line-by-line.
  
- Both `grok <http://logstash.net/docs/1.4.2/filters/grok>`__ and 
  `syslog <http://en.wikipedia.org/wiki/Syslog>`__ record formats can now be used when 
  :ref:`setting the format of a Stream <http-restful-api-stream-setting-properties>`
  (`CDAP-1949 <https://issues.cask.co/browse/CDAP-1949>`__).
  
- Added HTTP RESTful endpoints for listing Datasets and Streams as used by Adapters, 
  Programs, and Applications, and vice-versa 
  (`CDAP-2214 <https://issues.cask.co/browse/CDAP-2214>`__).
  
- Created a `queue introspection tool <https://github.com/caskdata/cdap/pull/2290>`__, 
  for counting processed and unprocessed entries in a 
  Flowlet queue (`CDAP-2105 <https://issues.cask.co/browse/CDAP-2105>`__).

- Support for CDAP SDK VM build automation has been added (`CDAP-2030 <https://issues.cask.co/browse/CDAP-2030>`__).

- A Cube Dataset has been added (`CDAP-1520 <https://issues.cask.co/browse/CDAP-1520>`__).

- A Batch and realtime Cube dataset sink has been added (`CDAP-1520 <https://issues.cask.co/browse/CDAP-1966>`__).

- Metrics and status information for MapReduce on a task level is now exposed (`CDAP-1520 <https://issues.cask.co/browse/CDAP-1958>`__).


.. _new-user-interface-300:

New User Interface
------------------
- Introduced a new UI, organization based on namespaces and users.
- Users can switch between namespaces. 
- Uses web sockets to retrieve updates from the backend.
- **Development Section**

  - Introduces a UI for programs based on run-ids.
  - Users can view logs and, in certain cases |---| Flows |---| Flowlets, of a program based on run ids.
  - Shows a list of Datasets and Streams used by a program, and shows programs using a specific Dataset and Stream.
  - Shows the history of a program (list of runs).
  - Datasets or Streams are explorable on a Dataset/Stream level or on a global level.
  - Shows program level metrics on under each program.
  
- **Operations section**

  - Introduces an operations section to explore metrics.
  - Allows users to create custom dashboard with custom metrics.
  - Users can add different types of charts (line, bar, area, pie, donut, scatter, spline,
    area-spline, area-spline-stacked, area-stacked, step, table).
  - Users can add multiple metrics on a single dashboard, or on a single widget on a single dashboard.
  - Users can organize the widgets in either a two, three, or four-column layout.
  - Users can toggle the frequency at which data is polled for a metric.
  - Users can toggle the resolution of data displayed in a graph.
  
- **Admin Section**

  - Users can manage different objects of CDAP (Applications, Programs, Datasets, and Streams).
  - Users can create namespaces.
  - Through the Admin view, users can configure their preferences at the CDAP level, namespace level, or Application level.
  - Users can manage the System Services, Applications, and Streams through the Admin view.
  
- **Adapters**

  - Users can create ETLBatch and ETLRealtime Adapters from within the UI.
  - Users can choose from a list of Plugins that comes by default with CDAP when creating an Adapter.
  - Users can save an Adapter as a draft, to be created at a later point-in-time.
  - Users can configure plugin properties with appropriate editors from within the UI when creating an Adapter.
  
- The Old CDAP Console has been deprecated.

Improvement
-----------

- The :ref:`Metrics system APIs<http-restful-api-metrics>` have been revised and improved
  (`CDAP-1596 <https://issues.cask.co/browse/CDAP-1596>`__).
- The Metrics system performance has been improved
  (`CDAP-2124 <https://issues.cask.co/browse/CDAP-2124>`__, 
  `CDAP-2125 <https://issues.cask.co/browse/CDAP-2125>`__).

Bug Fixes
---------

- The CDAP Authentication server now reports the port correctly when the port is set to 0
  (`CDAP-614 <https://issues.cask.co/browse/CDAP-614>`__).

- History of the programs running under Workflow (Spark and MapReduce) is now updated correctly
  (`CDAP-1293 <https://issues.cask.co/browse/CDAP-1293>`__).

- Programs running under a Workflow now receive a unique ``run-id``
  (`CDAP-2025 <https://issues.cask.co/browse/CDAP-2025>`__).

- RunRecords are now updated with the RuntimeService to account for node failures
  (`CDAP-2202 <https://issues.cask.co/browse/CDAP-2202>`__).

- MapReduce metrics are now available on a secure cluster
  (`CDAP-64 <https://issues.cask.co/browse/CDAP-64>`__).

API Changes
-----------

- The endpoint (``POST '<base-url>/metrics/search?target=childContext[&context=<context>]'``)
  that searched for the available contexts of metrics has been deprecated, pending removal
  in a later version of CDAP (`CDAP-1998 <https://issues.cask.co/browse/CDAP-1998>`__). A
  :ref:`replacement endpoint <http-restful-api-metrics-search-for-contexts>` is available.

- The endpoint (``POST '<base-url>/metrics/search?target=metric&context=<context>'``)
  that searched for metrics in a specified context has been deprecated, pending removal
  in a later version of CDAP (`CDAP-1998 <https://issues.cask.co/browse/CDAP-1998>`__). A
  :ref:`replacement endpoint <http-restful-api-metrics-search-for-metrics>` is available.

- The endpoint (``POST '<base-url>/metrics/query?context=<context>[&groupBy=<tags>]&metric=<metric>&<time-range>'``)
  that queried for a metric has been deprecated, pending removal
  in a later version of CDAP (`CDAP-1998 <https://issues.cask.co/browse/CDAP-1998>`__). A
  :ref:`replacement endpoint <http-restful-api-metrics-querying-a-metric>` is available.
  
- Metrics: The tag name for Service Handlers in previous releases was wrongly ``"runnable"``,
  and internally represented as ``"srn"``. These metrics are now tagged as ``"handler"`` (``"hnd"``), and
  metrics queries will only account for this tag name. If you need to query historic metrics
  that were emitted with the old tag ``"runnable"``, use ``"srn"`` to query them (instead of either
  ``"runnable"`` or ``"handler"``).

- The :ref:`CDAP CLI <cli>` startup options have been changed to accommodate a new option
  of executing a file containing a series of CLI commands, line-by-line.

- The metrics system APIs have been improved (`CDAP-1596 <https://issues.cask.co/browse/CDAP-1596>`__).

- The rules for :ref:`resolving resolution <http-restful-api-metrics-time-range>`
  when using ``resolution=auto`` in metrics query have been changed
  (`CDAP-1922 <https://issues.cask.co/browse/CDAP-1922>`__).

- Backward incompatible changes in ``InputFormatProvider`` and ``OutputFormatProvider``. 
  It won't affect user code that uses ``FileSet`` or ``PartitionedFileSet``. 
  It only affects classes who implement the ``InputFormatProvider`` or ``OutputFormatProvider``:

  - ``InputFormatProvider.getInputFormatClass()`` is removed and
  
    - replaced with ``InputFormatProvider.getInputFormatClassName()``;
    
  - ``OutputFormatProvider.getOutputFormatClass()`` is removed and
  
    - replaced with ``OutputFormatProvider.getOutputFormatClassName()``.


Deprecated and Removed Features
-------------------------------

- The `File DropZone <http://docs.cask.co/cdap/2.8.0/en/developers-manual/ingesting-tools/cdap-file-drop-zone.html>`__ 
  and `File Tailer <http://docs.cask.co/cdap/2.8.0/en/developers-manual/ingesting-tools/cdap-file-tailer.html>`__
  are no longer supported as of Release 3.0.
- Support for *Procedures* has been removed. After upgrading, an Application that
  contained a Procedure must be redeployed. 
- Support for *Service Workers* have been removed. After upgrading, an Application that
  contained a Service Worker must be redeployed.  
- The Old CDAP Console has been deprecated.
- Support for JDK/JRE 1.6 (Java 6) has ended; JDK/JRE 1.7 (Java 7) is 
  :ref:`now required for CDAP <install-java-runtime>` or the 
  :ref:`CDAP SDK <standalone-index>`.


.. _known-issues-300:

Known Issues
------------

- CDAP has been tested on and supports CDH 4.2.x through CDH 5.3.x, HDP 2.0 through 2.1, and
  Apache Bigtop 0.8.0. It has not been tested on more recent versions of CDH. 
  See :ref:`our Hadoop/HBase Environment configurations <install-hadoop-hbase>`.
  
- After upgrading CDAP from a pre-3.0 version, any unprocessed metrics data in Kafka will
  be lost and *WARN* log messages will be logged that tell about the inability to process
  old data in the old format.

- See the above section (*API Changes*) for alterations that can affect existing installations.

- See also the *Known Issues* of `version 2.8.0 <#known-issues-280>`_\ .


`Release 2.8.0 <http://docs.cask.co/cdap/2.8.0/index.html>`__
=============================================================

General
-------

- The HTTP RESTful API v2 was deprecated, replaced with the
  :ref:`namespaced HTTP RESTful API v3 <http-restful-api-v3>`.

- Added log rotation for CDAP programs running in YARN containers
  (`CDAP-1295 <https://issues.cask.co/browse/CDAP-1295>`__).

- Added the ability to submit to non-default YARN queues to provide 
  :ref:`resource guarantees <resource-guarantees>`
  for CDAP Master Services, CDAP Programs, and Explore Queries
  (`CDAP-1417 <https://issues.cask.co/browse/CDAP-1417>`__).

- Added the ability to :ref:`prune invalid transactions <tx-maintenance>`
  (`CDAP-1540 <https://issues.cask.co/browse/CDAP-1540>`__).

- Added the ability to specify 
  :ref:`custom logback file for CDAP programs <application-logback>`
  (`CDAP-1100 <https://issues.cask.co/browse/CDAP-1100>`__).

- System HTTP services now bind to all interfaces (0.0.0.0), rather than 127.0.0.1.

New Features
------------

- **Command Line Interface (CLI)**

  - CLI can now directly connect to a CDAP instance of your choice at startup by using
    ``cdap-cli.sh --uri <uri>``.
  - Support for runtime arguments, which can be listed by running ``"cdap-cli.sh --help"``.
  - Table rendering can be configured using ``"cli render as <alt|csv>"``. 
    The option ``"alt"`` is the default, with ``"csv"`` available for copy & pasting.
  - Stream statistics can be computed using ``"get stream-stats <stream-id>"``.
  
- **Datasets**

  - Added an ObjectMappedTable Dataset that maps object fields to table columns and that is also explorable.
  - Added a PartitionedFileSet Dataset that allows addressing files by meta data and that is also explorable.  
  - Table Datasets now support a multi-get operation for batched reads.
  - Allow an unchecked Dataset upgrade upon application deployment
    (`CDAP-1574 <https://issues.cask.co/browse/CDAP-1574>`__).

- **Metrics**

  - Added new APIs for exploring available metrics, including drilling down into the context of emitted metrics
  - Added the ability to explore (search) all metrics; previously, this was restricted to custom user metrics
  - There are new APIs for querying metrics
  - New capability to break down a metrics time series using the values of one or more tags in its context
  
- **Namespaces**

  - Applications and Programs are now managed within namespaces.
  - Application logs are available within namespaces.
  - Metrics are now collected and queried within namespaces.
  - Datasets can now created and managed within namespaces.
  - Streams are now namespaced for ingestion, fetching, and consuming by programs.
  - Explore operations are now namespaced.
  
- **Preferences**

  - Users can store preferences (a property map) at the instance, namespace, application, 
    or program level.
  
- **Spark**

  - Spark now uses a configurer-style API for specifying
    (`CDAP-382 <https://issues.cask.co/browse/CDAP-1134>`__).
  
- **Workflows**

  - Users can schedule a Workflow based on increments of data being ingested into a Stream.
  - Workflows can be stopped.
  - The execution of a Workflow can be forked into parallelized branches.
  - The runtime arguments for Workflow can be scoped.
  
- **Workers**

  - Added :ref:`Worker <workers>`, a new Program type that can be added to CDAP Applications, 
    used to run background processes and (beta feature) can write to Streams through the
    WorkerContext.
    
- **Upgrade and Data Migration Tool**

  - Added an :ref:`automated upgrade tool <install-upgrade>` which supports upgrading from
    2.6.x to 2.8.0. (**Note:** Apps need to be both recompiled and re-deployed.)
    Upgrade from 2.7.x to 2.8.0 is not currently supported. If you have a use case for it, 
    please reach out to us at `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`__.
  - Added a metric migration tool which migrates old metrics to the new 2.8 format.


Improvement
-----------

- Improved Flow performance and scalability with a new distributed queue implementation.


API Changes
-----------

- The endpoint (``GET <base-url>/data/explore/datasets/<dataset-name>/schema``) that
  retrieved the schema of a Dataset's underlying Hive table has been removed
  (`CDAP-1603 <https://issues.cask.co/browse/CDAP-1603>`__).
- Endpoints have been added to retrieve the CDAP version and the current configurations of
  CDAP and HBase (:ref:`Configuration HTTP RESTful API <http-restful-api-configuration>`).


.. _known-issues-280:

Known Issues
------------

- See also the *Known Issues* of `version 2.7.1 <#known-issues-271>`_\ .
- If the Hive Metastore is restarted while the CDAP Explore Service is running, the 
  Explore Service remains alive, but becomes unusable. To correct, :ref:`restart the CDAP Master
  <install-starting-services>`, which will restart all services 
  (`CDAP-1007 <https://issues.cask.co/browse/CDAP-1007>`__).
- User datasets with names starting with ``"system"`` can potentially cause conflicts
  (`CDAP-1587 <https://issues.cask.co/browse/CDAP-1587>`__).
- Scaling the number of metrics processor instances doesn't automatically distribute the
  processing load to the newer instances of the metrics processor. The CDAP Master needs to be
  restarted to effectively distribute the processing across all metrics processor instances
  (`CDAP-1853 <https://issues.cask.co/browse/CDAP-1853>`__).
- Creating a dataset in a non-existent namespace manifests in the RESTful API with an
  incorrect error message (`CDAP-1864 <https://issues.cask.co/browse/CDAP-1864>`__).
- Retrieving multiple metrics |---| by issuing an HTTP POST request with a JSON list as
  the request body that enumerates the name and attributes for each metric |---| is currently not
  supported in the 
  `Metrics HTTP RESTful API v3 <http://docs.cask.co/cdap/2.8.0/en/reference-manual/http-restful-api/http-restful-api-v3/metrics.html#query-tips>`__.
  Instead, use the v2 API. It will be supported in a future release.
- Typically, Datasets are bundled as part of Applications. When an Application is upgraded and redeployed,
  any changes in Datasets will not be redeployed. This is because Datasets can be shared across applications,
  and an incompatible schema change can break other applications that are using the Dataset.
  A workaround (`CDAP-1253 <https://issues.cask.co/browse/CDAP-1253>`__) is to allow *unchecked Dataset upgrades*.
  Upgrades cause the Dataset metadata, i.e. its specification including properties, to be updated. The Dataset
  runtime code is also updated. To prevent data loss the existing data and the underlying HBase tables remain as-is.

  You can allow *unchecked Dataset upgrades* by setting the configuration property ``dataset.unchecked.upgrade``
  to ``true`` in ``cdap-site.xml``. This will ensure that Datasets are upgraded when the Application is redeployed.
  When this configuration is set, the recommended process to deploy an upgraded Dataset is to first stop
  all Applications that are using the Dataset before deploying the new version of the Application.
  This lets all containers (Flows, Services, etc) to pick up the new Dataset changes.
  When Datasets are upgraded using ``dataset.unchecked.upgrade``, no schema compatibility checks are performed by the
  system. Hence it is very important that the developer verify the backward-compatibility, and makes sure that
  other Applications that are using the Dataset can work with the new changes.


`Release 2.7.1 <http://docs.cask.co/cdap/2.7.1/index.html>`__
=============================================================

API Changes
-----------
-  The property ``security.auth.server.address`` has been deprecated and replaced with
   ``security.auth.server.bind.address`` (`CDAP-639 <https://issues.cask.co/browse/CDAP-639>`__,
   `CDAP-1078 <https://issues.cask.co/browse/CDAP-1078>`__).

New Features
------------

- **Spark**

  - Spark now uses a configurer-style API for specifying (`CDAP-382 <https://issues.cask.co/browse/CDAP-382>`__).
  - Spark can now run as a part of a Workflow (`CDAP-465 <https://issues.cask.co/browse/CDAP-465>`__).

- **Security**

  - CDAP Master now obtains and refreshes Kerberos tickets programmatically (`CDAP-1134 <https://issues.cask.co/browse/CDAP-1134>`__).

- **Datasets**

  - A new, experimental dataset type to support time-partitioned File sets has been added.
  - Time-partitioned File sets can be queried with Impala on CDH distributions (`CDAP-926 <https://issues.cask.co/browse/CDAP-926>`__).
  - Streams can be made queryable with Impala by deploying an adapter that periodically
    converts it into partitions of a time-partitioned File set (`CDAP-1129 <https://issues.cask.co/browse/CDAP-1129>`__).
  - Support for different levels of conflict detection: ``ROW``, ``COLUMN``, or ``NONE`` (`CDAP-1016 <https://issues.cask.co/browse/CDAP-1016>`__).
  - Removed support for ``@DisableTransaction`` (`CDAP-1279 <https://issues.cask.co/browse/CDAP-1279>`__).
  - Support for annotating a Stream with a schema (`CDAP-606 <https://issues.cask.co/browse/CDAP-606>`__).
  - A new API for uploading entire files to a Stream has been added (`CDAP-411 <https://issues.cask.co/browse/CDAP-411>`__).

- **Workflow**

  - Workflow now uses a configurer-style API for specifying (`CDAP-1207 <https://issues.cask.co/browse/CDAP-1207>`__).
  - Multiple instances of a Workflow can be run concurrently (`CDAP-513 <https://issues.cask.co/browse/CDAP-513>`__).
  - Programs are no longer part of a Workflow; instead, they are added in the application
    and are referenced by a Workflow using their names (`CDAP-1116 <https://issues.cask.co/browse/CDAP-1116>`__).
  - Schedules are now at the application level and properties can be specified for
    Schedules; these properties will be passed to the scheduled program as runtime
    arguments (`CDAP-1148 <https://issues.cask.co/browse/CDAP-1148>`__).

.. _known-issues-271:

Known Issues
------------
- See also the *Known Issues* of `version 2.6.1. <#known-issues-261>`_
- When upgrading an existing CDAP installation to 2.7.1, all metrics are reset.


`Release 2.6.1 <http://docs.cask.co/cdap/2.6.1/index.html>`__
=============================================================

CDAP Bug Fixes
--------------
- Allow an *unchecked Dataset upgrade* upon application deployment
  (`CDAP-1253 <https://issues.cask.co/browse/CDAP-1253>`__).
- Update the Hive Dataset table when a Dataset is updated
  (`CDAP-71 <https://issues.cask.co/browse/CDAP-71>`__).
- Use Hadoop configuration files bundled with the Explore Service
  (`CDAP-1250 <https://issues.cask.co/browse/CDAP-1250>`__).

.. _known-issues-261:

Known Issues
------------
- See also the *Known Issues* of `version 2.6.0. <#known-issues-260>`_

- Typically, Datasets are bundled as part of Applications. When an Application is upgraded and redeployed,
  any changes in Datasets will not be redeployed. This is because Datasets can be shared across applications,
  and an incompatible schema change can break other applications that are using the Dataset.
  A workaround (`CDAP-1253 <https://issues.cask.co/browse/CDAP-1253>`__) is to allow *unchecked Dataset upgrades*.
  Upgrades cause the Dataset metadata, i.e. its specification including properties, to be updated. The Dataset
  runtime code is also updated. To prevent data loss the existing data and the underlying HBase tables remain as-is.

  You can allow *unchecked Dataset upgrades* by setting the configuration property ``dataset.unchecked.upgrade``
  to ``true`` in ``cdap-site.xml``. This will ensure that Datasets are upgraded when the Application is redeployed.
  When this configuration is set, the recommended process to deploy an upgraded Dataset is to first stop
  all Applications that are using the Dataset before deploying the new version of the Application.
  This lets all containers (Flows, Services, etc) to pick up the new Dataset changes.
  When Datasets are upgraded using ``dataset.unchecked.upgrade``, no schema compatibility checks are performed by the
  system. Hence it is very important that the developer verify the backward-compatibility, and makes sure that
  other Applications that are using the Dataset can work with the new changes.


`Release 2.6.0 <http://docs.cask.co/cdap/2.6.0/index.html>`__
=============================================================

API Changes
-----------
-  API for specifying Services and MapReduce programs has been changed to use a "configurer" 
   style; this will require modification of user classes implementing either MapReduce
   or Service as the interfaces have changed (`CDAP-335
   <https://issues.cask.co/browse/CDAP-335>`__).


New Features
------------

- **General**

  - Health checks are now available for CDAP system services
    (`CDAP-663 <https://issues.cask.co/browse/CDAP-663>`__).

- **Applications**

  -  Jar deployment now uses a chunked request and writes to a local temp file
     (`CDAP-91 <https://issues.cask.co/browse/CDAP-91>`__).

- **MapReduce**

  -  MapReduce programs can now read binary stream data
     (`CDAP-331 <https://issues.cask.co/browse/CDAP-331>`__).

- **Datasets**

  - Added :ref:`FileSet <datasets-fileset>`, a new core dataset type for working with sets of files
    (`CDAP-1 <https://issues.cask.co/browse/CDAP-1>`__).

- **Spark**

  - Spark programs now emit system and custom user metrics
    (`CDAP-346 <https://issues.cask.co/browse/CDAP-346>`__).
  - Services can be called from Spark programs and its worker nodes
    (`CDAP-348 <https://issues.cask.co/browse/CDAP-348>`__).
  - Spark programs can now read from Streams
    (`CDAP-403 <https://issues.cask.co/browse/CDAP-403>`__).
  - Added Spark support to the CDAP CLI (Command-line Interface)
    (`CDAP-425 <https://issues.cask.co/browse/CDAP-425>`__).
  - Improved speed of Spark unit tests
    (`CDAP-600 <https://issues.cask.co/browse/CDAP-600>`__).
  - Spark Programs now display system metrics in the CDAP Console
    (`CDAP-652 <https://issues.cask.co/browse/CDAP-652>`__).

- **Procedures**

  - Procedures have been deprecated in favor of Services
    (`CDAP-413 <https://issues.cask.co/browse/CDAP-413>`__).

- **Services**

  - Added an HTTP endpoint that returns the endpoints a particular Service exposes
    (`CDAP-412 <https://issues.cask.co/browse/CDAP-412>`__).
  - Added an HTTP endpoint that lists all Services
    (`CDAP-469 <https://issues.cask.co/browse/CDAP-469>`__).
  - Default metrics for Services have been added to the CDAP Console
    (`CDAP-512 <https://issues.cask.co/browse/CDAP-512>`__).
  - The annotations ``@QueryParam`` and ``@DefaultValue`` are now supported in custom Service handlers
    (`CDAP-664 <https://issues.cask.co/browse/CDAP-664>`__).

- **Metrics**

  - System and User Metrics now support gauge metrics
    (`CDAP-484 <https://issues.cask.co/browse/CDAP-484>`__).
  - Metrics can be queried using a Program’s run-ID
    (`CDAP-620 <https://issues.cask.co/browse/CDAP-620>`__).

- **Documentation**

  - A :ref:`Quick Start Guide <installation-quick-start>` has been added to the 
    :ref:`CDAP Administration Manual <admin-index>` 
    (`CDAP-695 <https://issues.cask.co/browse/CDAP-695>`__).

CDAP Bug Fixes
--------------

- Fixed a problem with readless increments not being used when they were enabled in a Dataset
  (`CDAP-383 <https://issues.cask.co/browse/CDAP-383>`__).
- Fixed a problem with applications, whose Spark or Scala user classes were not extended
  from either ``JavaSparkProgram`` or ``ScalaSparkProgram``, failing with a class loading error
  (`CDAP-599 <https://issues.cask.co/browse/CDAP-599>`__).
- Fixed a problem with the :ref:`CDAP upgrade tool <install-upgrade>` not preserving—for 
  tables with readless increments enabled—the coprocessor configuration during an upgrade
  (`CDAP-1044 <https://issues.cask.co/browse/CDAP-1044>`__).
- Fixed a problem with the readless increment implementation dropping increment cells when 
  a region flush or compaction occurred (`CDAP-1062 <https://issues.cask.co/browse/CDAP-1062>`__).

.. _known-issues-260:

Known Issues
------------

- When running secure Hadoop clusters, metrics and debug logs from MapReduce programs are
  not available (`CDAP-64 <https://issues.cask.co/browse/CDAP-64>`__ and `CDAP-797
  <https://issues.cask.co/browse/CDAP-797>`__).
- When upgrading a cluster from an earlier version of CDAP, warning messages may appear in
  the master log indicating that in-transit (emitted, but not yet processed) metrics
  system messages could not be decoded (*Failed to decode message to MetricsRecord*). This
  is because of a change in the format of emitted metrics, and can result in a small
  amount of metrics data points being lost (`CDAP-745
  <https://issues.cask.co/browse/CDAP-745>`__).
- Writing to datasets through Hive is not supported in CDH4.x
  (`CDAP-988 <https://issues.cask.co/browse/CDAP-988>`__).
- A race condition resulting in a deadlock can occur when a TwillRunnable container
  shutdowns while it still has Zookeeper events to process. This occasionally surfaces when
  running with OpenJDK or JDK7, though not with Oracle JDK6. It is caused by a change in the
  ``ThreadPoolExecutor`` implementation between Oracle JDK6 and OpenJDK/JDK7. Until Twill is
  updated in a future version of CDAP, a work-around is to kill the errant process. The Yarn
  command to list all running applications and their ``app-id``\s is::
  
    yarn application -list -appStates RUNNING

  The command to kill a process is::
  
    yarn application -kill <app-id>
    
  All versions of CDAP running Twill version 0.4.0 with this configuration can exhibit this
  problem (`TWILL-110 <https://issues.apache.org/jira/browse/TWILL-110>`__).


`Release 2.5.2 <http://docs.cask.co/cdap/2.5.2/index.html>`__
=============================================================

CDAP Bug Fixes
--------------

- Fixed a problem with a Coopr-provisioned secure cluster failing to start due to a classpath
  issue (`CDAP-478 <https://issues.cask.co/browse/CDAP-478>`__).
- Fixed a problem with the WISE app zip distribution not packaged correctly; a new version
  (0.2.1) has been released (`CDAP-533 <https://issues.cask.co/browse/CDAP-533>`__).
- Fixed a problem with the examples and tests incorrectly using the ByteBuffer.array
  method when reading a Stream event (`CDAP-549 <https://issues.cask.co/browse/CDAP-549>`__).
- Fixed a problem with the Authentication Server so that it can now communicate with an LDAP
  instance over SSL (`CDAP-556 <https://issues.cask.co/browse/CDAP-556>`__).
- Fixed a problem with the program class loader to allow applications to use a different
  version of a library than the one that the CDAP platform uses; for example, a different
  Kafka library (`CDAP-559 <https://issues.cask.co/browse/CDAP-559>`__).
- Fixed a problem with CDAP master not obtaining new delegation tokens after running for 
  ``hbase.auth.key.update.interval`` milliseconds (`CDAP-562 <https://issues.cask.co/browse/CDAP-562>`__).
- Fixed a problem with the transaction not being rolled back when a user service handler throws an exception 
  (`CDAP-607 <https://issues.cask.co/browse/CDAP-607>`__).

Other Changes
-------------

- Improved the CDAP documentation:

  - Re-organized the documentation into three manuals—Developers' Manual, Administration
    Manual, Reference Manual—and a set of examples, how-to guides and tutorials;
  - Documents are now in smaller chapters, with numerous updates and revisions;
  - Added a link for downloading an archive of the documentation for offline use;
  - Added links to examples relevant to a particular component;
  - Added suggested deployment architectures for Distributed CDAP installations;
  - Added a glossary;
  - Added navigation aids at the bottom of each page; and
  - Tested and updated the Standalone CDAP examples and their documentation.

Known Issues
------------
- Currently, applications that include Spark or Scala classes in user classes not extended
  from either ``JavaSparkProgram`` or ``ScalaSparkProgram`` (depending upon the language)
  fail with a class loading error. Spark or Scala classes should not be used outside of the
  Spark program. (`CDAP-599 <https://issues.cask.co/browse/CDAP-599>`__)
- See also the *Known Issues* of `version 2.5.0. <#known-issues-250>`_
- See also the *TWILL-110 Known Issue* of `version 2.6.0. <#known-issues-260>`_

`Release 2.5.1 <http://docs.cask.co/cdap/2.5.1/index.html>`__
=============================================================

CDAP Bug Fixes
--------------

- Improved the documentation of the CDAP Authentication and Stream Clients, both Java and Python APIs.
- Fixed problems with the CDAP Command Line Interface (CLI):

  - Did not work in non-interactive mode;
  - Printed excessive debug log messages;
  - Relative paths did not work as expected; and 
  - Failed to execute SQL queries.
  
- Removed dependencies on SNAPSHOT artifacts for *netty-http* and *auth-clients*. 
- Corrected an error in the message printed by the startup script ``cdap.sh``.
- Resolved a problem with the reading of the properties file by the CDAP Flume Client of CDAP Ingest library
  without first checking if authentication was enabled.

Other Changes
-------------

- The scripts ``send-query.sh``, ``access-token.sh`` and ``access-token.bat`` has been replaced by the 
  :ref:`CDAP Command Line Interface, <cli>` ``cdap-cli.sh``.
- The CDAP Command Line Interface now uses and saves access tokens when connecting to a secure CDAP instance.
- The CDAP Java Stream Client now allows empty String events to be sent.
- The CDAP Python Authentication Client's ``configure()`` method now takes a dictionary rather than a filepath.

Known Issues
------------
- See *Known Issues* of `the previous version. <#known-issues-250>`_
- See also the *TWILL-110 Known Issue* of `version 2.6.0. <#known-issues-260>`_


`Release 2.5.0 <http://docs.cask.co/cdap/2.5.0/index.html>`__
=============================================================

New Features
------------

Ad-hoc querying
.................
- Capability to write to Datasets using SQL
- Added a CDAP JDBC driver allowing connections from Java applications and third-party business intelligence tools
- Ability to perform ad-hoc queries from the CDAP Console:

  - Execute a SQL query from the Console
  - View list of active, completed queries
  - Download query results

Datasets
.................
- Datasets can be tested with TestBase outside of the context of an Application
- CDAP now checks Datasets for compatibility in a verification stage
- The Transaction engine uses server-side filtering for efficient transactional reads
- Dataset specifications can now be dynamically reconfigured through the use of RESTful endpoints
- The Bundle jar format is now used for Dataset libs
- Increments on Datasets are now read-less

Services
.................
- Added simplified APIs for using Services from other programs such as MapReduce, Flows and Procedures
- Added an API for creating Services and handlers that can use Datasets transactionally
- Added a RESTful API to make requests to a Service via the Router

Security
.................
- Added authorization logging
- Added Kerberos authentication to Zookeeper secret keys
- Added support for SSL

Spark Integration
.................
- Supports running Spark programs as a part of CDAP applications in Standalone mode
- Supports running Spark programs written with Spark versions 1.0.1 or 1.1.0 
- Supports Spark's *MLib* and *GraphX* modules
- Includes three examples demonstrating CDAP Spark programs
- Adds display of Spark program logs and history in the CDAP Console

Streams
.................
- Added a collection of applications, tools and APIs specifically for the ETL (Extract, Transform and Loading) of data
- Added support for asynchronously writing to Streams

Clients
.................
- Added a Command Line Interface
- Added a Java Client Interface


Major CDAP Bug Fixes
--------------------
- Fixed a problem with a HADOOP_HOME exception stacktrace when unit-testing an Application
- Fixed an issue with Hive creating directories in /tmp in the Standalone and unit-test frameworks
- Fixed a problem with type inconsistency of Service API calls, where numbers were showing up as strings
- Fixed an issue with the premature expiration of long-term Authentication Tokens
- Fixed an issue with the Dataset size metric showing data operations size instead of resource usage


.. _known-issues-250:

Known Issues
------------
- Metrics for MapReduce programs aren't populated on secure Hadoop clusters
- The metric for the number of cores shown in the Resources view of the CDAP Console will be zero
  unless YARN has been configured to enable virtual cores
- See also the *TWILL-110 Known Issue* of `version 2.6.0. <#known-issues-260>`_
