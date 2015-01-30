.. meta::
    :author: Cask Data, Inc 
    :description: Release notes for the Cask Data Application Platform
    :copyright: Copyright © 2014 Cask Data, Inc.

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

`Release 2.6.1 <http://docs.cask.co/cdap/2.6.1/index.html>`__
=============================================================

CDAP Bug Fixes
--------------
- Allow unchecked Dataset upgrade on application deploy
  (`CDAP-1253 <https://issues.cask.co/browse/CDAP-1253>`__).
- Update Hive Dataset table when a dataset is updated
  (`CDAP-71 <https://issues.cask.co/browse/CDAP-71>`__).
- Use Hadoop configuration files bundled with Explore Service
  (`CDAP-1250 <https://issues.cask.co/browse/CDAP-1250>`__).

Known Issues
------------

- Typically Datasets are bundled as part of Applications. When an Application is upgraded and redeployed,
  any changes in Datasets will not be redeployed. This is because Datasets can be shared across applications,
  and an incompatible schema change can break other applications that are using the Dataset.
  A workaround (`CDAP-1253 <https://issues.cask.co/browse/CDAP-1253>`__) is to allow unchecked Dataset upgrades.
  Upgrades cause the Dataset metadata i.e. it's specification, including properties, to be updated. The Dataset
  runtime code is also updated. To prevent data loss the existing data and the underlying HBase tables remain as is.

  You can allow unchecked Dataset upgrades by setting the configuration property ``dataset.unchecked.upgrade``
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
-  API for specifying Services and MapReduce Jobs has been changed to use a "configurer" 
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

  -  MapReduce jobs can now read binary stream data
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
- Currently, when a CDAP application is deleted, all metrics are deleted along with the
  application. Depending on the amount of metrics data to be deleted, an application
  deletion timeout can result. A recent change (`CDAP PR-928
  <https://github.com/caskdata/cdap/pull/928>`__) attempts to address this problem by only
  deleting aggregated metrics when an application is deleted. A side effect of this change
  is that if you re-deploy an application immediately after deletion, you may still see
  metrics from the previous session on timeseries charts. (`CDAP-1124
  <https://issues.cask.co/browse/CDAP-1124>`__).
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
- Metrics for MapReduce jobs aren't populated on secure Hadoop clusters
- The metric for the number of cores shown in the Resources view of the CDAP Console will be zero
  unless YARN has been configured to enable virtual cores
- See also the *TWILL-110 Known Issue* of `version 2.6.0. <#known-issues-260>`_
