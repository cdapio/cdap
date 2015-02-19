.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _http-restful-api-metrics:

===========================================================
Metrics HTTP RESTful API
===========================================================

.. highlight:: console

As Applications process data, CDAP collects metrics about the Application’s behavior and
performance. Some of these metrics are the same for every Application—how many events are
processed, how many data operations are performed, etc.—and are thus called system or CDAP
metrics.

Other metrics are user-defined and differ from Application to Application. 
For details on how to add metrics to your Application, see the section on 
:ref:`User-Defined Metrics <operations-metrics>` in
the :ref:`CDAP Administration Manual. <admin-index>`


Metric Searches and Metric Queries
----------------------------------

The process of retrieving a metric involves two steps: first, obtain the correct context
for the metric, and second, querying for the metric giving the correct context and parameters.

Metrics data is identified as a combination of the two: the metrics context and the metric name.

Metrics context is hierarchal, rooted in the CDAP instance, and extending through
namespaces, applications, and down to the individual elements.


Search Available Metrics
------------------------

To search the available metrics, perform an HTTP POST request::

  POST <base-url>/metrics/search?target=<target>[&context=<context>]
  

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<target>``
     - ``childContext``
   * - ``<context>`` (Optional)
     - Metrics context to search within. If not provided, the search is provided across
       all contexts.
       
.. rubric:: Examples

.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``GET <base-url>/search?target=<target>[&context=<context>]/``
       
   * - Description
     - Using a *System* metric, *process.busyness*
   * - 
     - 
   * - HTTP Method
     - ``GET <base-url>/metrics/user/apps/HelloWorld/flows/``
       ``WhoFlow/runs/13ac3a50-a435-49c8-a752-83b3c1e1b9a8/flowlets/saver/names.bytes?aggregate=true``
   * - Description
     - Querying the *User-Defined* metric *names.bytes*, of the Flow *saver*, by its run-ID
   * - 
     - 
   * - HTTP Method
     - ``GET <base-url>/metrics/user/apps/HelloWorld/services/``
       ``WhoService/runnables/WhoRun/names.bytes?aggregate=true``
   * - Description
     - Using a *User-Defined* metric, *names.bytes* in a Service's Handler

Query Available Metrics
------------------------

to be completed.


