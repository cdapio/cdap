.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2016 Cask Data, Inc.

.. _logging-monitoring:

======================
Logging and Monitoring
======================

Logs and Metrics
================
CDAP collects logs and metrics for all of its internal services and and user applications.
Being able to view these details can be very helpful in debugging CDAP applications as
well as analyzing their performance. CDAP gives access to its logs, metrics, and other
monitoring information through RESTful APIs as well as a Java Client.

See the :ref:`Logging <http-restful-api-logging>`, :ref:`Metrics <http-restful-api-metrics>`, 
and :ref:`Monitoring <http-restful-api-monitor>` HTTP RESTful APIs, the :ref:`Java Client
<reference:java-client-api>`, and the :ref:`master-service-logging-configuration` for
additional information.


Log Location
============
The location of logs depends on the mode of CDAP and the Hadoop distribution.

- With **Standalone CDAP** (the CDAP SDK), logs are located in ``<CDAP-SDK-HOME>/logs``.

- For **Distributed CDAP**, with the exception of Cloudera Manager, logs are located in
  ``/var/log/cdap``. With Cloudera Manager installations, log files are located in
  directories under ``/var/run/cloudera-scm-agent/process``.


Logging Messages
================

.. highlight:: java

CDAP supports logging through standard `SLF4J (Simple Logging Facade for Java)
<http://www.slf4j.org/manual.html>`__ APIs. For instance, in a flowlet you can write::

  private static Logger LOG = LoggerFactory.getLogger(WordCounter.class);
  ...
  @ProcessInput
  public void process(String line) {
    LOG.info("{}: Received line {}", this.getContext().getTransactionAwareName(), line);
    ... // processing
    LOG.info("{}: Emitting count {}", this.getContext().getTransactionAwareName(), wordCount);
    output.emit(wordCount);
  }

The log messages emitted by your application code can be viewed in different ways.

- Using the :ref:`restful-api`. The :ref:`Logging HTTP RESTful API <http-restful-api-logging>` 
  details the available contexts that can be called to retrieve different messages.
- Log messages of an application can be viewed in the :ref:`CDAP UI <cdap-ui>`
  by clicking the *Logs* tab of an application.


.. _master-service-logging-configuration:

Master Service Logging Configuration
====================================

:term:`Master Services <master services>` use ``logback-container.xml``, packaged with the CDAP distribution,
for logging configuration. This logback does log rotation once a day at midnight and expires logs older than
14 days. Changes can be made to ``logback-container.xml``, and the ``cdap-master`` process will need to be restarted
for the modified logback to take effect.


Monitoring Utilities
====================
CDAP can be monitored using external systems such as `Nagios <https://www.nagios.org/>`__; a Nagios-style plugin 
`is available <https://github.com/caskdata/cdap-monitoring-tools/blob/develop/nagios/README.rst>`__
for checking the status of CDAP applications, programs, and the CDAP instance itself.

