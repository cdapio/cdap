.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

.. _operations-dashboard:

======================
Dashboard and Reports
======================

**Dashboard** is a real-time interactive interface that visualizes program run statistics,
over any 24-hour window in the last week or, for a more detailed view, over any 1-hour window.

**Reports** provide comprehensive insights into program runs. You can generate a report to view
statistics about program runs within a given time range such as 24 hours or one week. A Spark batch
job will process the program runs and generate the requested report.

Dashboard and reports both support monitoring **across namespaces**, allowing you to view dashboard and generate report statistics across multiple namespaces.

Dashboard
=========

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

Reports
=======
Reports is a tool for administrators to take a historical look at their applications'
program runs, and performance, optionally filtered by criteria such as program type or final run status.

Reports must be enabled before you can use it, This deploys the **cdap-program-report** application to system namespace
and starts a Spark service, This service enables you to generate, list, view, save and delete reports.

Once you have enabled the reports and you are in reports page, you can request a report by providing the following

  - Filter, you can specify the filters you are interested in such as program-type (pipeline, custom-program) or program run status
  - Add additional namespaces to monitor and include in the report, by default report will be generated for programs in current namespace.
  - Specify the column fields you want to be included in the report content such as namespace, start-time, start-method, etc.
  - Select a time range.
  - Generate the report.

Once you request a report, the Spark service launches a Spark job to process the program run statistics
in a scalable way and generate the report summary and report details, this could take a few minutes before the report is ready to be viewed.

Reports contain two components: a summary and a program runs table. They provide, for example, this information:

  - Tabular view of program runs that matche your request filters, with selected column fields.
  - In multi-tenant deployments, a summary and detailed report of pipelines run by each user
  - Minimum, maximum and average duration of program runs that match the report request filters
  - Summary of runs grouped by namespace, application type, start method and user.

By default, reports expire 2 days after their creation time, unless they are explicitly saved.

Limitations
-----------
  - Reports is **not** supported on clusters with Spark version lower than 2.1.
  - Only the program runs that were started in or after CDAP-5.0 will be processed and included in the reports, program runs from earlier CDAP version will be skipped.

More information about using the dashboard can be found at :ref:`Dashboard REST API <http-restful-api-dashboard>`
and Reports application's usage can be found at :ref:`Reports REST API <http-restful-api-reports>` in the REST API documentation.
