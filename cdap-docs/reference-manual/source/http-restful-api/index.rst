.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2015-2017 Cask Data, Inc.

:hide-toc: true

.. _http-restful-api:
.. _restful-api:
.. _http-restful-api-v3:

========================
CDAP HTTP RESTful API v3
========================

.. toctree::
   
    Introduction <introduction>
    Artifact <artifact>
    Configuration <configuration>
    Dataset <dataset>
    Lifecycle <lifecycle>
    Logging <logging>
    Metadata <metadata>
    Metadata Management <metadata-management>
    Metrics <metrics>
    Monitor <monitor>
    Namespace <namespace>
    Preferences <preferences>
    Query <query>
    Route Config <routeconfig>
    Security <security>
    Service <service>
    Stream <stream>
    Transactions <transactions>
    Views <views>
    Workflow <workflow>

.. highlight:: console

The Cask Data Application Platform (CDAP) has an HTTP interface for a multitude of
purposes: from sending data events to a stream and inspecting the contents of a
stream through checking the status of system and custom CDAP services. V3 of the
API includes the namespacing of applications, data, and metadata to achieve application and
data isolation. This is an inital step towards introducing `multi-tenancy
<http://en.wikipedia.org/wiki/Multitenancy>`__ into CDAP.

**Introduction**

- :doc:`Introduction: <introduction>` conventions, converting from HTTP RESTful API v2,
  naming restrictions, status codes, and working with CDAP security

**General APIs**

- :doc:`Namespace: <namespace>` creating and managing namespaces
- :doc:`Metadata: <metadata>` setting, retrieving, and deleting user metadata annotations
- :doc:`Metadata Management: <metadata-management>` metadata management
- :doc:`Preferences: <preferences>` setting, retrieving, and deleting preferences
- :doc:`Configuration: <configuration>` retrieving the CDAP and HBase configurations
- :doc:`Security: <security>` granting, revoking and listing privileges on CDAP entities,
  managing secure storage
- :doc:`Transactions: <transactions>` interacting with the transaction service

**Major CDAP Entities APIs**

- :doc:`Artifact: <artifact>` deploying artifacts and retrieving details about plugins
  available to artifacts and classes contained within artifacts
- :doc:`Lifecycle: <lifecycle>` deploying and managing applications, and managing the
  lifecycle of flows, MapReduce programs, Spark programs, workflows, and custom services
- :doc:`Stream: <stream>` sending data events to a stream or to inspect the contents of a stream
- :doc:`Dataset: <dataset>` interacting with datasets, dataset modules, and dataset types
- :doc:`Service: <service>` supports making requests to the methods of an application’s services
- :doc:`Route Config: <query>` create, fetch, and delete route configurations (*route
  configs*) which allocate requests between different versions of a service
- :doc:`Workflow: <workflow>` retrieving values from workflow tokens and statistics on workflow runs

**Querying and Viewing APIs**

- :doc:`Query: <query>` sending ad-hoc queries to CDAP datasets
- :doc:`Views: <views>` a read-only view of a stream, with a specific read format

**Logging, Metrics, and Monitoring APIs**

- :doc:`Logging: <logging>` retrieving application logs
- :doc:`Metrics: <metrics>` retrieving metrics for system and user applications (user-defined metrics)
- :doc:`Monitor: <monitor>` checking the status of various system and custom CDAP services


.. rubric:: Alphabetical List of APIs

- :doc:`Introduction: <introduction>` conventions, converting from HTTP RESTful API v2,
  naming restrictions, status codes, and working with CDAP security

..

- :doc:`Artifact: <artifact>` deploying artifacts and retrieving details about plugins
  available to artifacts and classes contained within artifacts
- :doc:`Configuration: <configuration>` retrieving the CDAP and HBase configurations
- :doc:`Dataset: <dataset>` interacting with datasets, dataset modules, and dataset types
- :doc:`Lifecycle: <lifecycle>` deploying and managing applications, and managing the
  lifecycle of flows, MapReduce programs, Spark programs, workflows, and custom services
- :doc:`Logging: <logging>` retrieving application logs
- :doc:`Metadata: <metadata>` setting, retrieving, and deleting user metadata annotations
- :doc:`Metadata Management: <metadata-management>` metadata management
- :doc:`Metrics: <metrics>` retrieving metrics for system and user applications (user-defined metrics)
- :doc:`Monitor: <monitor>` checking the status of various system and custom CDAP services
- :doc:`Namespace: <namespace>` creating and managing namespaces
- :doc:`Preferences: <preferences>` setting, retrieving, and deleting preferences
- :doc:`Query: <query>` sending ad-hoc queries to CDAP datasets
- :doc:`Route Config: <query>` create, fetch, and delete route configurations (*route
  configs*) which allocate requests between different versions of a service
- :doc:`Security: <security>` granting, revoking, and listing privileges, as well as adding, retrieving,
  and managing *Secure Keys*
- :doc:`Service: <service>` supports making requests to the methods of an application’s services
- :doc:`Stream: <stream>` sending data events to a stream or to inspect the contents of a stream
- :doc:`Transactions: <transactions>` interacting with the transaction service
- :doc:`Views: <views>` a read-only view of a stream, with a specific read format
- :doc:`Workflow: <workflow>` retrieving values from workflow tokens and statistics on workflow runs
