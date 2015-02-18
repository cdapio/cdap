.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

:hide-toc: true

.. _restful-v2-api:
.. _http-restful-api-v2:

===========================================================
CDAP HTTP RESTful API v2
===========================================================

.. toctree::
   
    Introduction <introduction>
    Lifecycle <lifecycle>
    Stream <stream>
    Dataset <dataset>
    Query <query>
    Procedure <procedure>
    Service <service>
    Logging <logging>
    Metrics <metrics>
    Monitor <monitor>

.. include:: /_includes/include-v280-deprecate-http-restful-api-v2.rst

.. highlight:: console

The Cask Data Application Platform (CDAP) has an HTTP interface for a multitude of purposes:

- :doc:`Introduction: <introduction>` conventions, status codes, and working with CDAP Security
- :doc:`Lifecycle: <lifecycle>` deploying and managing Applications and managing the lifecycle of Flows,
  Procedures, MapReduce Programs, Workflows, and Custom Services
- :doc:`Stream: <stream>` sending data events to a Stream or to inspect the contents of a Stream
- :doc:`Dataset: <dataset>` interacting with Datasets, Dataset Modules, and Dataset Types
- :doc:`Query: <query>` sending ad-hoc queries to CDAP Datasets
- :doc:`Procedure: <procedure>` sending calls to a stored Procedure
- :doc:`Service: <service>` supports making requests to the methods of an Application’s Services
- :doc:`Logging: <logging>` retrieving Application logs
- :doc:`Metrics: <metrics>` retrieving metrics for system and user Applications (user-defined metrics)
- :doc:`Monitor: <monitor>` checking the status of various System and Custom CDAP services
