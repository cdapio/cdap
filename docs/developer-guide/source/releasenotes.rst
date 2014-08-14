.. :author: Cask, Inc 
   :description: Release notes for the Cask Data Application Platform

.. _overview_release-notes:

.. index::
   single: Release Notes

==================
CDAP Release Notes
==================
.. _release-notes:

Release 2.4.0
=============

New Features
------------
- To allow external programs access to the services hosted by Custom Services, service discovery 
  is exposed through the RESTful end-points
- Hive CLIService has started successfully
- Redesign Explore client so that it returns a Future object
- Allow variables in SQL Statements
- Allow additional options in JDBC connection URL
- Ability to perform ad-hoc queries from the CDAP Console:

  - Execute a SQL query from the Console
  - View list of active, completed queries
  - Download query results
  
- Datasets can be tested with TestBase outside of the context of an Application
- CDAP now checks Datasets for compatibility in a verification stage
- The Transaction engine uses server-side filtering for efficient transactional reads
- Dataset specifications can now be dynamically reconfigured through the use of RESTful endpoints
- The RESTful endpoint now can modify Dataset instance specifications
- The Bundle jar format is now used for Dataset libs
- Increments on Datasets are now read-less
- Added a Command-line Interface
- Added a Java Client Interface

Major CDAP Bug Fixes
--------------------
- Fixed a problem with a HADOOP_HOME exception stacktrace when unit-testing an Application
- Fixed an issue with Hive creating directories in /tmp in the Singlenode and unit-test frameworks
- Fixed a problem with type inconsistency of Service API calls, where numbers were showing up as strings
- Fixed an issue with the premature expiration of long-term Authentication Tokens
- Fixed a problem with the cliService returning a double when an integer was stored

Other CDAP Changes
------------------
- `A list of deprecated Interfaces, Classes and Methods <javadocs/deprecated-list.html>`__ 
  is included in the Javadocs
  
Known Issues
------------
- Metrics for MapReduce jobs aren't populated on secure Hadoop clusters
- The metric for the number of cores shown in the Resources view of the CDAP Console will be zero
  unless YARN has been setup to enable virtual cores
