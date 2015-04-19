.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Scaling Instances
============================================

You can scale CDAP components using:

- the :ref:`Scaling<http-restful-api-lifecycle-scale>` methods of the 
  :ref:`Lifecycle HTTP RESTful API<http-restful-api-lifecycle>`;
- the :ref:`ProgramClient API<program-client>` of the 
  :ref:`Java Client API<java-client-api>`; or
- the :ref:`Get/Set Commands<cli-available-commands>` of the 
  :ref:`Command Line Interface<cli>`.

The examples given below use the :ref:`Lifecycle HTTP RESTful API<http-restful-api-lifecycle-scale>`.

.. highlight:: console

Scaling Flowlets
----------------
You can query and set the number of instances executing a given Flowlet
by using the ``instances`` parameter with HTTP GET and PUT methods::

  GET /v3/namespaces/default/apps/<app-id>/flows/<flow-id>/flowlets/<flowlet-id>/instances
  PUT /v3/namespaces/default/apps/<app-id>/flows/<flow-id>/flowlets/<flowlet-id>/instances

with the arguments as a JSON string in the body::

  { "instances" : <quantity> }

Where:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<app-id>``
     - Name of the application
   * - ``<flow-id>``
     - Name of the Flow
   * - ``<flowlet-id>``
     - Name of the Flowlet
   * - ``<quantity>``
     - Number of instances to be used

Example: Find out the number of instances of the Flowlet *saver* in
the Flow *WhoFlow* of the application *HelloWorld*::

  GET /v3/namespaces/default/apps/HelloWorld/flows/WhoFlow/flowlets/saver/instances

Example: Change the number of instances of the Flowlet *saver*
in the Flow *WhoFlow* of the application *HelloWorld*::

  PUT /v3/namespaces/default/apps/HelloWorld/flows/WhoFlow/flowlets/saver/instances

with the arguments as a JSON string in the body::

  { "instances" : 2 }


Scaling Services
------------------

In a similar way to `Scaling Flowlets`_, you can query or change the number of handler instances of a Service
by using the ``instances`` parameter with HTTP GET and PUT methods::

  GET /v3/namespaces/default/apps/<app-id>/services/<service-id>/instances
  PUT /v3/namespaces/default/apps/<app-id>/services/<service-id>/instances

with the arguments as a JSON string in the body::

  { "instances" : <quantity> }

Where:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<app-id>``
     - Name of the application
   * - ``<service-id>``
     - Name of the Service
   * - ``<quantity>``
     - Number of handler instances requested
  
Example: Find out the number of handler instances of the Service *RetrieveCounts*
of the application *WordCount*::

  GET /v3/namespaces/default/apps/WordCount/services/RetrieveCounts/instances

Example: Change the number of handler instances of the Service *RetrieveCounts*
of the application *WordCount*::

  PUT /v3/namespaces/default/apps/WordCount/services/RetrieveCounts/instances

with the arguments as a JSON string in the body::

  { "instances" : 2 }
  
Example using the :ref:`CDAP Standalone SDK <standalone-index>` and ``curl`` (reformatted to fit)::

  curl -w'\n' -X PUT 'http://localhost:10000/v3/namespaces/default/apps/WordCount/services/RetrieveCounts/instances' \
    -d '{ "instances" : 2 }'
