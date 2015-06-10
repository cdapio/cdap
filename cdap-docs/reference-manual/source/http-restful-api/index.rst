.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2015 Cask Data, Inc.

:hide-toc: true

.. _http-restful-api:
.. _restful-api:
.. _http-restful-api-v3:

========================
CDAP HTTP RESTful API v3
========================

.. toctree::
   
    Introduction <introduction>
    Namespace <namespace>
    Lifecycle <lifecycle>
    Configuration <configuration>
    Preferences <preferences>
    Application Templates and Adapters <apptemplates>
    Stream <stream>
    Dataset <dataset>
    Query <query>
    Service <service>
    Logging <logging>
    Metrics <metrics>
    Monitor <monitor>
    Transactions <transactions>

.. highlight:: console

The Cask Data Application Platform (CDAP) has an HTTP interface for a multitude of
purposes: everything from sending data events to a stream or to inspect the contents of a
stream through checking the status of various system and custom CDAP services. V3 of the
API includes the namespacing of applications, data and metadata to achieve application and
data isolation. This is an inital step towards introducing `multi-tenancy
<http://en.wikipedia.org/wiki/Multitenancy>`__ into CDAP.

- :doc:`Introduction: <introduction>` conventions, converting from HTTP RESTful API v2, 
  naming restrictions, status codes, and working with CDAP security
- :doc:`Namespace: <namespace>` creating and managing namespaces
- :doc:`Lifecycle: <lifecycle>` deploying and managing applications, and managing the lifecycle of flows,
  MapReduce programs, Spark programs, workflows, and custom services
- :doc:`Configuration: <configuration>` retrieving the CDAP and HBase configurations
- :doc:`Preferences: <preferences>` setting, retrieving, and deleting preferences
- :doc:`Application Templates and Adapters: <apptemplates>` obtaining available application templates and
  plugins, and creating, deleting, and managing the lifecycle of adapters
- :doc:`Stream: <stream>` sending data events to a stream or to inspect the contents of a stream
- :doc:`Dataset: <dataset>` interacting with datasets, dataset modules, and dataset types
- :doc:`Query: <query>` sending ad-hoc queries to CDAP datasets
- :doc:`Service: <service>` supports making requests to the methods of an application’s services
- :doc:`Logging: <logging>` retrieving application logs
- :doc:`Metrics: <metrics>` retrieving metrics for system and user applications (user-defined metrics)
- :doc:`Monitor: <monitor>` checking the status of various system and custom CDAP services
- :doc:`Transactions: <transactions>` interacting with the transaction service
