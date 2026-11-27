.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

.. _operations-dashboard:

=========
Dashboard
=========

**Dashboard** is a real-time interactive interface that visualizes program run statistics,
over any 24-hour window in the last week or, for a more detailed view, over any 1-hour window.

Dashboard supports monitoring **across namespaces**, allowing you to view dashboard statistics across multiple namespaces.

Overview
========

The **Dashboard** was introduced in CDAP 5.0 to offer a holistic operational view of all pipelines and
application programs running in a CDAP instance. It addresses these common operational needs:

  - CDAP System status
      - Filter and view pipelines and/or custom programs that are currently running
      - View pipelines and/or custom programs that have been submitted but are not yet running in the cluster
      - Slice and dice by namespace, program-status and start-method.

  - Predicting future resource usage
      - View program and pipeline runs scheduled in the future to predict and proactively manage resource usage.

**Note** : Dashboard UI offers both charting and tabular view of the program runs status in the 24-hour window.

Limitation
----------
  - Only the program runs that were started in or after CDAP-5.0 will be processed and displayed on dashboard.

More information about using the dashboard can be found at :ref:`Dashboard REST API <http-restful-api-dashboard>`
in the REST API documentation.
