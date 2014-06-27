.. :Author: Continuuity, Inc 
   :Description: Release notes for Continuuity Reactor

.. _overview_release-notes:

.. index::
   single: Release Notes

=============
Release Notes
=============
.. _release-notes:

New Application, Stream, Flowlet, and Dataset Features
======================================================
- New Application API with an easier and cleaner way to define application components
- Stream support for retention policy for its data; configurable at runtime, 
  while the Stream is in use
- Stream truncate support via REST
- Simplified Flowlet ``@Batch`` support: process methods don't require an ``Iterator`` as a parameter
- New Datasets API that exposes low level APIs to give more power & flexibility for developing custom
  Dataset types; flexible dataset types configuration
- Datasets types management outside of applications: a REST interface to add, remove, and discover
  Dataset types
- Datasets management outside of application: a REST interface to create, truncate, drop and discover
  Datasets

New Ad-hoc Querying Feature
===========================
- Reactor now supports ad-hoc SQL queries over Datasets
- A new API that allows developers to expose the schema of the Dataset and make it query able
- A new REST API to submit SQL queries over Datasets and retrieve the results

New Security Features
=====================
- Reactor now supports perimeter security, restricting access to resources to only authenticated users
- With ``security.enabled=true``, users must then login in order to access the Reactor UI
- Access to all Reactor REST APIs can be secured by an ``OAuth 2`` Bearer token, which is obtained by
  authentication with the Reactor authentication service using a pluggable mechanism
- The Reactor authentication service supports authentication via either LDAP or a JASPI plugin 
  out of the  box and can be extended to other mechanisms through a simple plug-in API
- Access to the Reactor authentication service can be secured by enabling SSL support

New Reactor Services Features
=============================
- Reactor application now supports adding Custom User Services  
- Custom user services can be discovered from Flows, Procedures and MapReduce jobs
- The number of user service instances can be scaled
- Facility to see into Reactor system components via the Reactor Dashboard
- The number of Reactor system component instances can be scaled via the Reactor Dashboard

Documentation Changes
=====================
- New developer guides (`Querying <query.html>`__ and `Security <security.html>`_) with new features
- Additional sections on Datasets added to the `Advanced guide <advanced.html#datasets-system>`_ 
  explaining new features
- `Programming Guide <programming.html>`_ was restructured for easier access to its sections
- Sidebar with the Table of Contents was made "sticky" to help navigate longer documents

Major Reactor Bug Fixes
=======================

- Fixed a problem with empty log directories not being deleted after the log files in them were deleted
- Fixed an issue in the Reactor Dashboard's Metric Explorer, where the user interface controls for 
  selecting metrics were enabled inappropriately

Other Reactor Changes
=====================
- `A list of deprecated Interfaces, Classes and Methods <javadocs/deprecated-list.html>`__ 
  is included in the Javadocs

.. For later
.. - The term "DataSet" has been standardized in both the APIs and the documentation as either 
..  "Dataset" or "dataset", depending on the context, to reduce errors from the internal capitalization

