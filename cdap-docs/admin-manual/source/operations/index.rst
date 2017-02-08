.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

:hide-toc: true

.. _operations-index:

==========
Operations
==========

.. toctree::
   :maxdepth: 1
   
    Logging and Monitoring <logging>
    Metrics <metrics>
    Preferences and Runtime Arguments <preferences>
    Scaling Instances <scaling-instances>
    Resource Guarantees in YARN <resource-guarantees>
    Transaction Service Maintenance <tx-maintenance>
    CDAP UI <cdap-ui>


.. |logging| replace:: **Logging and Monitoring:**
.. _logging: logging.html

- |logging|_ CDAP collects **logs** for all of its internal services and user
  applications; at the same time, CDAP can be **monitored through external systems**. Covers
  **log location**, **logging messages**, the **master services logback configuration** and
  **CDAP support for logging** through standard SLF4J (Simple Logging Facade for Java) APIs.


.. |metrics| replace:: **Metrics:**
.. _metrics: metrics.html

- |metrics|_ CDAP collects **metrics about the application’s behavior and performance**.
  

.. |preferences| replace:: **Preferences and Runtime Arguments:**
.. _preferences: preferences.html

- |preferences|_ **Preferences** provide the ability to save configuration information. 
  Flows, MapReduce and Spark programs, services, workers, and workflows can receive **runtime arguments.**


.. |scaling-instances| replace:: **Scaling Instances:**
.. _scaling-instances: scaling-instances.html

- |scaling-instances|_ Covers **querying and setting the number of instances of flowlets and services.** 


.. |resource-guarantees| replace:: **Resource Guarantees:**
.. _resource-guarantees: resource-guarantees.html

- |resource-guarantees|_ Providing resource guarantees **for CDAP programs in YARN.**


.. |tx-maintenance| replace:: **Transaction Service Maintenance:**
.. _tx-maintenance: tx-maintenance.html

- |tx-maintenance|_ Periodic maintenance of the **Transaction Service.**


.. |cdap-ui| replace:: **CDAP UI:**
.. _cdap-ui: cdap-ui.html

- |cdap-ui|_ The CDAP UI is available for **deploying, querying, and managing CDAP.** 


.. rubric:: Command Line Interface

Most of the administrative operations are also available more conveniently through the
Command Line Interface. See :ref:`reference:cli` in the 
:ref:`CDAP Reference Manual<reference:reference-index>` for details.


.. rubric:: Getting a Health Check

.. _operations-health-check:

.. highlight:: console

Administrators can check the health of various services in the system. (In these examples,
substitute for ``<host>`` the host name or IP address of the CDAP server. Substitute the
correct ports if they have been changed from the default values shown.) On success, these
calls return a valid HTTP response with a 200 code.

- To retrieve the **health check of the CDAP UI**, make a GET request to the URI (at the
  ``dashboard.bind.port``)::

    http://<host>:9999/status
  
  Such as (for the Standalone CDAP):
   
  .. tabbed-parsed-literal::

    $ curl -w"\n" -X GET "http://localhost:9999/status"


- To retrieve the **health check of the CDAP Router**, make a GET request to the URI (at
  the ``router.bind.port``)::

    http://<host>:10000/status

  Such as (for the Standalone CDAP):
   
  .. tabbed-parsed-literal::

    $ curl -w"\n" -X GET "http://localhost:10000/status"

- To retrieve the **health check of the CDAP Authentication Server**, make a GET request to
  the URI (at the ``security.auth.server.bind.port``)::
  
    http://<host>:10009/status

  Such as (for the Standalone CDAP):
   
  .. tabbed-parsed-literal::

    $ curl -w"\n" -X GET "http://localhost:10009/status"

- To retrieve a **list of health checks of all the CDAP system services running in YARN**, make a GET request
  to the URI (at the ``router.bind.port``)::
  
    http://<host>:10000/v3/system/services

  Such as (for the Standalone CDAP):
   
  .. tabbed-parsed-literal::

    $ curl -w"\n" -X GET "http://localhost:10000/v3/system/services"

  On success, the call returns a JSON string with component names, their corresponding 
  statuses, and details of the services (reformatted to fit)::
  
    [{"name":"appfabric","description":"Service for managing application
      lifecycle.","status":"OK","logs":"OK","min":1,"max":1,"requested":1,"provisioned":1},
     {"name":"dataset.executor","description":"Service to perform dataset
      operations.","status":"OK","logs":"OK","min":1,"max":1,"requested":1,"provisioned":1},
     {"name":"explore.service","description":"Service to run ad-hoc
      queries.","status":"OK","logs":"OK","min":1,"max":1,"requested":1,"provisioned":1},
     {"name":"log.saver","description":"Service to collect and store
      logs.","status":"OK","logs":"NOTOK","min":1,"max":1,"requested":1,"provisioned":1},
     {"name":"metrics","description":"Service to handle metrics
      requests.","status":"OK","logs":"OK","min":1,"max":1,"requested":1,"provisioned":1},
     {"name":"metrics.processor","description":"Service to process application and system
      metrics.","status":"OK","logs":"NOTOK","min":1,"max":1,"requested":1,"provisioned":1},
     {"name":"streams","description":"Service that handles stream data
      ingestion.","status":"OK","logs":"OK","min":1,"max":1,"requested":1,"provisioned":1},
     {"name":"transaction","description":"Service that maintains transaction
      states.","status":"OK","logs":"NOTOK","min":1,"max":1,"requested":1,"provisioned":1}]

- To retrieve a **health check of all the CDAP system services running in YARN**, make a GET request
  to the URI (at the ``router.bind.port``)::
  
    http://<host>:10000/v3/system/services/status

  Such as (for the Standalone CDAP):
   
  .. tabbed-parsed-literal::

    $ curl -w"\n" -X GET "http://localhost:10000/v3/system/services/status"

- To retrieve a **health check of a particular CDAP system service running in YARN**, make a GET request
  to the URI (at the ``router.bind.port``), substituting for ``<service-id>``, as 
  shown in the :ref:`Monitor HTTP RESTful API <checking-the-status-of-a-system-service>`::
  
    http://<host>:10000/v3/system/services/<service-id>/status

  Such as (for the metrics service of the Standalone CDAP):
   
  .. tabbed-parsed-literal::

    $ curl -w"\n" -X GET "http://localhost:10000/v3/system/services/metrics/status"


