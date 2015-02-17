.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2015 Cask Data, Inc.

:hide-toc: true

.. _http-restful-api-v3:
.. _restful-api:

===========================================================
CDAP HTTP RESTful API v3
===========================================================

.. toctree::
   
    Introduction <introduction>
    Namespace <namespace>
    Lifecycle <lifecycle>
    Stream <stream>
    Dataset <dataset>
    Query <query>
    Procedure <procedure>
    Service <service>
    Logging <logging>
    Metrics <metrics>
    Monitor <monitor>
..    Summary <summary>

.. highlight:: console


The Cask Data Application Platform (CDAP) has an HTTP interface for a multitude of purposes:

- :doc:`Introduction: <introduction>` conventions, converting from HTTP RESTful API v2, 
  naming restrictions, status codes, and working with CDAP Security
- :doc:`Namespace: <namespace>` creating, listing, and deleting namespaces
- :doc:`Lifecycle: <lifecycle>` deploying and managing Applications, and managing the lifecycle of Flows,
  Procedures, MapReduce Programs, Workflows, and Custom Services
- :doc:`Stream: <stream>` sending data events to a Stream or to inspect the contents of a Stream
- :doc:`Dataset: <dataset>` interacting with Datasets, Dataset Modules, and Dataset Types
- :doc:`Query: <query>` sending ad-hoc queries to CDAP Datasets
- :doc:`Procedure: <procedure>` sending calls to a stored Procedure
- :doc:`Service: <service>` supports making requests to the methods of an Application’s Services
- :doc:`Logging: <logging>` retrieving Application logs
- :doc:`Metrics: <metrics>` retrieving metrics for system and user Applications (user-defined metrics)
- :doc:`Monitor: <monitor>` checking the status of various System and Custom CDAP services
.. - :doc:`Summary: <summary>` lists all API calls in a single page in summary form