.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _advanced-configuring-resources:

=============================
Configuring Program Resources
=============================

The amount of resources (virtual CPU cores and memory) of YARN containers is configurable
before the start of the run of any application components. By setting these resources at
runtime |---| instead of as part of the application code |---| you can remove the
requirement for re-compiling, re-packaging, and redeploying of an application in order to
simply change the resources used.

**Note:** Any resource specifications set directly in the application (such as shown in
:ref:`mapreduce-resources`) will be over-ridden as the runtime arguments :ref:`take
precedence <admin:preferences>`.

Virtual CPU Cores and Memory
============================
The runtime system looks for these arguments:

- ``system.resources.cores``: Number of virtual cores for each YARN container
- ``system.resources.memory``: Memory in MB for each YARN container

These arguments can be set either as a runtime argument (which affects just the next run of
a program) or as a preference (which affects all subsequent runs), as described in the
:ref:`Preferences HTTP RESTful API <http-restful-api-preferences>` documentation.

You can use the :ref:`Preferences HTTP RESTful API <http-restful-api-preferences>` to set
these arguments at various levels in the hierarchy (across a namespace, across programs in
an application, for a specific program in a specific application, etc.). These will be
persisted and used with each run of the application's programs.

Configuring Sub-components
==========================
For program types that have sub-components (such as flows, MapReduce and Spark programs),
a prefix can be added to limit the scope of the arguments.

**Flow**

- Prefix with ``flowlet.<flowletName>.`` to set resources for a particular flowlet in a flow

**MapReduce Program**

- Prefix with ``task.driver.`` to set resources for the MapReduce driver only
- Prefix with ``task.mapper.`` to set resources for the mappers only
- Prefix with ``task.reducer.`` to set resources for the reducers only

**Spark Program**

- Prefix with ``task.client.`` to set resources for the Spark client only
- Prefix with ``task.driver.`` to set resources for the Spark driver only
- Prefix with ``task.executor.`` to set resources for the Spark executors only

**Workflow**

- Prefix with ``mapreduce.<workflowNodeName>.`` to set resources for a particular MapReduce node in a workflow
- Prefix with ``spark.<workflowNodeName>.`` to set resources for a particular MapReduce node in a workflow

Examples
========
As an example, assume that you have deployed the :ref:`Purchase <examples-purchase>` example in CDAP
in the ``default`` namespace, and would like to set the memory used by its YARN containers.

- To set the memory used by the mappers of *all* MapReduce jobs, in *all* namespaces, as ``2048 MB``, you would use::

    task.mapper.system.resources.memory = 2048
  
  You could set this using the CDAP CLI:
  
    .. tabbed-parsed-literal::
       :tabs: "CDAP CLI"

       |cdap >| set preferences instance 'task.mapper.system.resources.memory=2048'
  
  or by using a ``curl`` call:

    .. tabbed-parsed-literal::

      $ curl -w"\n" -X PUT "http://example.com:10000/v3/preferences" \
          -H 'Content-Type: application/json' -d '{ "task.mapper.system.resources.memory": 2048 }'

- To set the memory used by the mapper of the *PurchaseHistoryBuilder* MapReduce job, you would use::

    task.mapper.system.resources.memory = 2048
  
  You could set this using the CDAP CLI:
  
    .. tabbed-parsed-literal::
       :tabs: "CDAP CLI"

       |cdap >| set preferences mapreduce 'task.mapper.system.resources.memory=2048' PurchaseHistory.PurchaseHistoryBuilder
  
  or by using a ``curl`` call:

    .. tabbed-parsed-literal::

      $ curl -w"\n" -X PUT "http://example.com:10000/v3/namespaces/default/apps/PurchaseHistory/mapreduce/PurchaseHistoryBuilder/preferences" \
          -H 'Content-Type: application/json' -d '{ "task.mapper.system.resources.memory": 2048 }'

- To set the memory used by the *collector* node of the *PurchaseFlow*, you would use::

    flowlet.collector.system.resources.memory = 1024
  
  You could set this using the CDAP CLI:
  
    .. tabbed-parsed-literal::
       :tabs: "CDAP CLI"

       |cdap >| set preferences flow 'flowlet.collector.system.resources.memory=1024' PurchaseHistory.PurchaseFlow
  
  or by using a ``curl`` call:

    .. tabbed-parsed-literal::

      $ curl -w"\n" -X PUT "http://example.com:10000/v3/namespaces/default/apps/PurchaseHistory/flows/PurchaseFlow/preferences" \
          -H 'Content-Type: application/json' -d '{ "flowlet.collector.system.resources.memory": 1024 }'

These configurations can also be set through the CDAP UI, either as preferences or runtime arguments.
