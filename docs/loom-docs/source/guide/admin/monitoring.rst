.. _guide_admin_monitoring:

.. index::
   single: Monitoring and Metrics
=======================
Monitoring and Metrics
=======================

.. include:: /guide/admin/admin-links.rst

Overview
========

This section outlines the tools that Loom provides to enable an administrator to monitor the system.

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

Loom UI
-------
The UI runs as a Node.js process. We recommend a standard http monitoring check on the /status endpoint of the Loom UI:
::
  http://<loom-host>:<loom-ui-port>/status

The UI should respond with "OK" and HTTP return code 200.


Metrics
=======


How to access

What metrics are available


Log Output
==========


Log files in Loom are, by default, written to ``/var/log/loom``. The output directory can be configured using the
``LOOM_LOG_DIR`` environment variable for each of the Loom services (for more information, see
:doc:`Installation Guide </guide/installation/index>`).

.. Temporarily commented out
..
  .. warning::
     It is important to note that Loom depends on the external Linux utility logrotate to rotate its logs. Loom
     packages contain logrotate configurations in ``/etc/logrotate.d`` but it does not perform the rotations itself.
     Please ensure logrotate is enabled on your Loom hosts.