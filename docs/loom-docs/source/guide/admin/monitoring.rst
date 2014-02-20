.. _guide_admin_monitoring:

.. index::
   single: Monitoring and Metrics
=======================
Monitoring and Metrics
=======================

.. include:: /guide/admin/admin-links.rst

Overview
========

This section outlines the tools that Loom provides to enable an administrator to monitor the system. Since the 
output from these tools and checks are easily parseable and since the output is written to standard out, writing a simple
parsing script and integrating with Nagios are relatively easy. 

Process Monitoring
==================

Loom provides HTTP endpoints to check for the status of the running processes. 

Loom Server
-----------
The Loom Server runs as a java process. Similar to the UI, we recommend a standard http monitoring check on the
``/status`` endpoint
::
  http://<loom-host>:<loom-server-port>/status

The server should respond with "OK" and HTTP return code 200.


Loom Provisioner
----------------
The Loom provisioner runs as an individual Ruby process on each provisioner. We recommend any standard process presence check. The Loom init script itself can be used:
::
  /etc/init.d/loom-provisioner status

This will display brief status for all configured provisioner process, and return non-zero if any one of them is not running.
Alternatively, any standard process presence monitoring can check for the process:
::
  /opt/loom/provisioner/embedded/bin/ruby /opt/loom/provisioner/daemon/provisioner.rb
  
An example of the output from the above run is:
::
  $ /etc/init.d/loom-provisioner status
   loom-provisioner 1 running as process 2436
   loom-provisioner 2 running as process 2437
   loom-provisioner 3 running as process 2438
   loom-provisioner 4 is not running
   loom-provisioner 5 pidfile exists, but process does not appear to be running
   At least one provisioner failed

Loom UI
-------
The UI runs as a Node.js process. We recommend a standard http monitoring check on the /status endpoint of the Loom UI:
::
  http://<loom-host>:<loom-ui-port>/status

The UI should respond with "OK" and HTTP return code 200.


Provisioning Metrics
====================
Loom provides a number of cluster provisioning metrics through the use of
`Java Management Extensions (JMX) <http://docs.oracle.com/javase/7/docs/technotes/guides/jmx/>`_.

By default, JMX support is disabled. To enable JMX, the administrator will need to uncomment out the following line in
``/etc/default/loom-server`` and can customize any of the options:
::
  export LOOM_JAVA_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9010
              -Dcom.sun.management.jmxremote.local.only=false
              -Dcom.sun.management.jmxremote.authenticate=false
              -Dcom.sun.management.jmxremote.ssl=false"

.. warning::
  The code shown above has no enabling of authentication and security. An administrator will have to customize these
  settings according to their needs. For more information on how to setup secure monitoring in JMX, see
  `this page <http://docs.oracle.com/javase/7/docs/technotes/guides/management/agent.html>`_.

Available Metrics
-----------------

The following is a list of top-level metrics available through the Loom JMX extension:

.. list-table::
   :widths: 20 15 75
   :header-rows: 1

   * - Metric
     - Type
     - Description
   * - QueueLength
     - Long
     - The current length of the queue
   * - ProvisionerStats
     - CompositeData
     - The number of provisioner actions that were dispatched
   * - FailedProvisionerStats
     - CompositeData
     - The number of provisioner actions that failed
   * - SuccessfulProvisionerStats
     - CompositeData
     - The number of provisioner actions that were successful
   * - ClusterStats
     - CompositeData
     - The number of cluster actions that were received
   * - FailedClusterStats
     - CompositeData
     - The number of cluster actions that failed
   * - SuccessfulClusterStats
     - CompositeData
     - The number of cluster actions that were successful

The provisioner and cluster metrics return a CompositeData object that contains multiple values, and are further
described below.

Provisioner Metrics
^^^^^^^^^^^^^^^^^^^

Below is a description of the metrics found in provisioner metrics, namely ``ProvisionerStats``,
``FailedProvisionerStats`` and ``SuccessfulProvisionerStats``:

.. list-table::
   :widths: 15 75
   :header-rows: 1

   * - Metric
     - Description
   * - bootstrap
     - Number of plugin specific preparation tasks for provisioning
   * - confirm
     - Number of confirmations after completing provider-specific validation/preparation
   * - create
     - Number of requests sent to a provider to initiate provisioning of a machine
   * - delete
     - Number of requests sent to a provider to delete a machine
   * - install
     - Number of requested actions to install services
   * - configure
     - Number of requested actions to configure services
   * - initialize
     - Number of requested actions to initialize services
   * - remove
     - Number of requested actions to remove services
   * - start
     - Number of requested actions to start services
   * - stop
     - Number of requested actions to stop services


Cluster Metrics
^^^^^^^^^^^^^^^

Below is a description of the metrics found in cluster metrics, namely
``ClusterStats``, ``FailedClusterStats`` and ``SuccessfulClusterStats``:

.. list-table::
   :widths: 15 75
   :header-rows: 1

   * - Metric
     - Description
   * - create
     - Number of requests to create a cluster
   * - delete
     - Number of requests to delete a cluster
   * - solve
     - Number of layout solving requests

Log Output
==========
By default, the log files in Loom are written to ``/var/log/loom``. The output directory can be configured using the
``LOOM_LOG_DIR`` environment variable for each of the Loom services (for more information, see
:doc:`Installation Guide </guide/installation/index>`).

