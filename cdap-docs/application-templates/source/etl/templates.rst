.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _apptemplates-etl-templates:

======================================
ETL Template and Plugin Details (Beta)
======================================

Introduction
============
Details of templates and the required properties for sources, transformations, and sinks
can be explored using RESTful APIs.

Note that while Adapters are namespaced, Templates and Plugins are not namespaced. If you
are creating a custom Plugin to add to either the existing Templates or your own Template,
its name needs to not collide with existing names.

Shipped with CDAP as part of the ETL Batch and ETL Realtime Application Templates, the
plugins listed below are available for creating ETL Adapters.


Application Template Details
============================

.. include:: ../../../reference-manual/source/http-restful-api/adapter.rst 
   :start-after: .. _http-restful-api-adapter-application-templates:
   :end-before:  .. _http-restful-api-adapter-adapters:
