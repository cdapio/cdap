.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _cask-hydrator-running-pipelines:

=================
Running Pipelines
=================


Pipeline can be started, stopped, and controlled using:

- :ref:`Cask Hydrator UI <cask-hydrator-running-pipelines-within-hydrator>`
- :ref:`CDAP UI <cdap-ui>`
- :ref:`CDAP CLI <cdap-cli>`
- Command line tools, using the :ref:`Lifecycle HTTP RESTful API <http-restful-api-lifecycle-start>`

.. _cask-hydrator-running-pipelines-within-hydrator

Running a Pipeline within Hydrator
==================================
From within Hydrator, you can start and stop pipelines.

For batch pipelines, you can start and stop their schedule. (To change their
schedules requires creating a new pipelines with a new schedule.)

For real-time pipelines, 



Re-running a Pipeline
=====================
*[New in 3.5: To Be Completed]*


Notifications
=============
To Be Completed


Logs
====
As pipelines run, they create entries in the CDAP logs.

Logs for a pipeline can be obtained using the same tools as any other CDAP application and
program, and are described in the :ref:`Administration manual, Logging and Monitoring
<logging-monitoring>` and the :ref:`Logging HTTP RESTful API <http-restful-api-logging>`.

Script transform steps can write to logs, as described in the section in developing
plugins on :ref:`script transformations
<cask-hydrator-creating-a-plugin-script-transformations>`.


Metrics
=======
As pipelines run, they create both system and user metrics.

System metrics for a pipeline can be obtained using the same tools as any other CDAP
application and program, and are described in the :ref:`Administration manual, metrics
<operations-metrics>` and the :ref:`Metrics HTTP RESTful API <http-restful-api-metrics>`.

Script transform steps can create metrics, as described in the section in developing
plugins on :ref:`script transformations
<cask-hydrator-creating-a-plugin-script-transformations>`.


Error Record Handling
=====================
To Be Completed


Configuring Resources
=====================
*To Be Completed*
