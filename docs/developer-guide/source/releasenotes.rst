.. :Author: Continuuity, Inc 
   :Description: Release notes for Continuuity Reactor

.. _overview_release-notes:

.. index::
   single: Release Notes

=============
Release Notes
=============
.. _release-notes:

Release 2.3.1
=============

Bug Fixes
---------
- Fixed a class loading error that occurs during application deployment when the Reactor 
  Master has incorrect classpath entries 


Release 2.3.0
=============

New Application, Stream, Flowlet, and Dataset Features
------------------------------------------------------
- New Application API with an easier and cleaner way to define application components
- Stream support for retention policy for its data; configurable at runtime, 
  while the Stream is in use
- Stream truncate support via REST
- Simplified Flowlet ``@Batch`` support: process methods don't require an ``Iterator`` as a parameter
- New `Datasets API <advanced.html#datasets-system>`__ that gives more power & flexibility for developing custom Datasets
- Datasets management outside of applications: a REST interface to create, truncate, drop and discover
  Datasets

New Ad-hoc Querying Feature
---------------------------
- Continuuity Reactor now supports `ad-hoc SQL queries over Datasets <query.html>`__
- A new API that allows developers to expose the schema of the Dataset and make it query-able
- A new REST API to submit SQL queries over Datasets and retrieve the results

New Security Features
---------------------
- Continuuity Reactor now supports `perimeter security, restricting access to resources only to authenticated users
  <security.html>`__
- With ``security.enabled=true``, users must then login in order to access the Reactor UI
- Access to all Reactor REST APIs can be secured by an ``OAuth 2`` Bearer token, which is obtained by
  authentication with the Reactor authentication service using a pluggable mechanism
- The Reactor authentication service supports authentication via either LDAP or a JASPI plugin 
  out of the  box and can be extended to other mechanisms through a simple plug-in API
- Access to the Reactor authentication service can be secured by enabling SSL support

New Reactor Services Features
-----------------------------
- Continuuity Reactor now supports adding Custom User Services  
- Custom User Services can be discovered from Flows, Procedures and MapReduce jobs
- The number of User Service instances can be scaled
- Facility to see into Reactor system components via the Reactor Dashboard
- The number of Reactor system component instances can be scaled via the Reactor Dashboard

Documentation Changes
---------------------
- `Programming Guide <programming.html>`__ was restructured for easier access to its sections
- Sidebar with the Table of Contents was made "sticky" to help navigate longer documents

Major Reactor Bug Fixes
-----------------------
- Fixed a problem with empty log directories not being deleted after the log files in them were deleted
- Fixed an issue in the Reactor Dashboard's Metric Explorer, where the user interface controls for 
  selecting metrics were enabled inappropriately

Other Reactor Changes
---------------------
- `A list of deprecated Interfaces, Classes and Methods <javadocs/deprecated-list.html>`__ 
  is included in the Javadocs
  
Known Issues
------------
- Metrics for MapReduce jobs aren't populated on secure Hadoop clusters
- The metric for the number of cores shown in the Resources view of the Reactor Dashboard will be zero
  unless YARN has been setup to enable virtual cores
