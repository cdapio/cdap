.. :author: Cask Data, Inc 
   :description: Release notes for the Cask Data Application Platform
   :copyright: Copyright © 2014 Cask Data, Inc.

.. _overview_release-notes:

.. index::
   single: Release Notes

============================================
Cask Data Application Platform Release Notes
============================================
.. _release-notes:

Release 2.5.1
=============

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
- Corrected errors in the documentation of the CDAP Application Case Study.
- Resolved a problem with the reading of the properties file by the CDAP Flume Client of CDAP Ingest library
  without first checking if authentication was enabled.

Other Changes
-------------

- The scripts ``send-query.sh``, ``access-token.sh`` and ``access-token.bat`` has been replaced by the 
  `CDAP Command Line Interface, <api.html#cli>`__ ``cdap-cli.sh``.
- The CDAP Command Line Interface now uses and saves access tokens when connecting to a secure CDAP instance.
- The `CDAP Java Stream Client <https://github.com/caskdata/cdap-ingest/tree/release/1.0.0/cdap-stream-clients/java>`__ 
  now allows empty String events to be sent.
- The `CDAP Python Authentication Client's <https://github.com/caskdata/cdap-clients/tree/release/1.0.0/cdap-authentication-clients/python>`__ 
  ``configure()`` method now takes a dictionary rather than a filepath.

Known Issues
------------
See *Known Issues* of `the previous version. <#known-issues-251>`_


Release 2.5.0
=============

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
- Added a Command-line Interface
- Added a Java Client Interface


Major CDAP Bug Fixes
--------------------
- Fixed a problem with a HADOOP_HOME exception stacktrace when unit-testing an Application
- Fixed an issue with Hive creating directories in /tmp in the Standalone and unit-test frameworks
- Fixed a problem with type inconsistency of Service API calls, where numbers were showing up as strings
- Fixed an issue with the premature expiration of long-term Authentication Tokens
- Fixed an issue with the Dataset size metric showing data operations size instead of resource usage


.. _known-issues-251:

Known Issues
------------
- Metrics for MapReduce jobs aren't populated on secure Hadoop clusters
- The metric for the number of cores shown in the Resources view of the CDAP Console will be zero
  unless YARN has been configured to enable virtual cores
