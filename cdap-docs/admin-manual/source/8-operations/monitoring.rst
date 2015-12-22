.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

===============
Monitoring CDAP
===============

Logs and Metrics
================
CDAP collects logs and metrics for all of its internal services. Being able to view these
details can be really helpful in debugging CDAP applications as well as analyzing their
performance. CDAP gives access to its logs, metrics, and other monitoring information
through RESTful APIs as well as a Java Client.

See the :ref:`Logging <http-restful-api-logging>`, :ref:`Metrics <http-restful-api-metrics>`,
and :ref:`Monitoring <http-restful-api-monitor>` HTTP APIs, and the
:ref:`Java Client <reference:java-client-api>` for more information.


Monitoring Utilities
====================
CDAP can be monitored using external systems such as `Nagios <https://www.nagios.org/>`__; a Nagios-style plugin 
`is available <https://github.com/caskdata/cdap-monitoring-tools/blob/develop/nagios/README.rst>`__
for checking the status of CDAP applications, programs, and the CDAP instance itself.
