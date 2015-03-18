.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2015 Cask Data, Inc.

.. highlight:: console

.. _http-restful-api-configuration:
.. _http-restful-api-v3-configuration:

======================================
Configuration Service HTTP RESTful API
======================================

The configurations of CDAP and HBase are exposed via HTTP RESTful endpoints and are documented here.

.. _http-restful-api-configuration-cdap:

CDAP Configurations
-------------------

To retrieve all the configurations used by CDAP, issue an HTTP GET request::

  GET <base-url>/config/cdap?format=<type>
  
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<type>``
     - Format for returned type, either ``json`` (default) or ``xml``
  
The response is a string in the specified format. For example, ::

  { "": "" }


.. _http-restful-api-configuration-hbase:

HBase Configurations
--------------------

To retrieve all the configurations used by HBase, issue an HTTP GET request::

  GET <base-url>/config/hbase?format=<type>
  
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<type>``
     - Format for returned type, either ``json`` (default) or ``xml``
  
The response is a string in the specified format. For example, ::

  { "": "" }

