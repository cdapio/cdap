.. :author: Cask Data, Inc 
   :description: Release notes for the Cask Data Application Platform

.. _overview_release-notes:

.. index::
   single: Release Notes

============================================
Cask Data Application Platform Release Notes
============================================
.. _release-notes:

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
- The RESTful endpoint now can modify Dataset instance specifications
- The Bundle jar format is now used for Dataset libs
- Increments on Datasets are now read-less

Services
.................
- Added simplified APIs for using Services from other programs such as MapReduce Jobs, Flows and Procedures
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
 
Streams
.................
- Added a collection of applications, tools and APIs specifically for the ETL (Extract, Transform and Loading) of data
- Added support for asynchronously writing to Streams

Management
.................
- Added a Command-line Interface
- Added a Java Client Interface


Major CDAP Bug Fixes
--------------------
- Fixed a problem with a HADOOP_HOME exception stacktrace when unit-testing an Application
- Fixed an issue with Hive creating directories in /tmp in the Singlenode and unit-test frameworks
- Fixed a problem with type inconsistency of Service API calls, where numbers were showing up as strings
- Fixed an issue with the premature expiration of long-term Authentication Tokens
- Fixed a problem with the cliService returning a double when an integer was stored
- Fixed an issue with the Dataset size metric showing data operations size instead of resource usage


Other CDAP Changes
------------------
- `A list of deprecated Interfaces, Classes and Methods <javadocs/deprecated-list.html>`__ 
  is included in the Javadocs
  
Known Issues
------------
- Metrics for MapReduce jobs aren't populated on secure Hadoop clusters
- The metric for the number of cores shown in the Resources view of the CDAP Console will be zero
  unless YARN has been configured to enable virtual cores
