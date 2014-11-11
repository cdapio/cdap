.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Scaling Instances
============================================

.. highlight:: console

Scaling Flowlets
----------------
You can query and set the number of instances executing a given Flowlet
by using the ``instances`` parameter with HTTP GET and PUT methods::

  GET /v2/apps/<app-id>/flows/<flow-id>/flowlets/<flowlet-id>/instances
  PUT /v2/apps/<app-id>/flows/<flow-id>/flowlets/<flowlet-id>/instances

with the arguments as a JSON string in the body::

  { "instances" : <quantity> }

Where:
  :<app-id>: Name of the application
  :<flow-id>: Name of the Flow
  :<flowlet-id>: Name of the Flowlet
  :<quantity>: Number of instances to be used

Example: Find out the number of instances of the Flowlet *saver* in
the Flow *WhoFlow* of the application *HelloWorld*::

  GET /v2/apps/HelloWorld/flows/WhoFlow/flowlets/saver/instances

Example: Change the number of instances of the Flowlet *saver*
in the Flow *WhoFlow* of the application *HelloWorld*::

  PUT /v2/apps/HelloWorld/flows/WhoFlow/flowlets/saver/instances

with the arguments as a JSON string in the body::

  { "instances" : 2 }


Scaling Procedures
------------------
In a similar way to `Scaling Flowlets`_, you can query or change the number of instances of a Procedure
by using the ``instances`` parameter with HTTP GET and PUT methods::

  GET /v2/apps/<app-id>/procedures/<procedure-id>/instances
  PUT /v2/apps/<app-id>/procedures/<procedure-id>/instances

with the arguments as a JSON string in the body::

  { "instances" : <quantity> }

Where:
  :<app-id>: Name of the application
  :<procedure-id>: Name of the Procedure
  :<quantity>: Number of instances to be used

Example: Find out the number of instances of the Procedure *saver*
in the Flow *WhoFlow* of the application *HelloWorld*::

  GET /v2/apps/HelloWorld/flows/WhoFlow/procedure/saver/instances

Example: Change the number of instances of the Procedure *saver*
in the Flow *WhoFlow* of the application *HelloWorld*::

  PUT /v2/apps/HelloWorld/flows/WhoFlow/procedure/saver/instances

with the arguments as a JSON string in the body::

  { "instances" : 2 }
