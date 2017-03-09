.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

.. _logging-monitoring:

======================
Logging and Monitoring
======================

Logs and Metrics
================
CDAP collects logs and metrics for all of its internal services and and user applications.
Being able to view these details can be very helpful in debugging CDAP applications as
well as analyzing their performance. CDAP gives access to its logs, metrics, and other
monitoring information through RESTful APIs, the CDAP UI, as well as a Java Client.

See the :ref:`Logging <http-restful-api-logging>`, :ref:`Metrics <http-restful-api-metrics>`, 
and :ref:`Monitoring <http-restful-api-monitor>` HTTP RESTful APIs, the :ref:`Java Client
<reference:java-client-api>`, :ref:`master-service-logging-configuration`, and the 
:ref:`application-logback` for additional information.

Logging Framework
=================

.. highlight:: console

CDAP collects logs of both its internal services and user applications. To do so, CDAP
uses a logging framework, as shown in this diagram:

.. figure:: /_images/logging-framework.png
    :figwidth: 100%
    :width: 800px
    :align: center

    **CDAP Logging Framework:** System and User Application Logs




based on `Logback <https://logback.qos.ch/manual>`__, consisting
of *appenders* and *logging pipelines*:

- An **appender** is a Java class, responsible for writing log events to persistent storage and
  maintaining metadata about that storage.  
  CDAP implements a ``RollingLocationLogAppender`` class that <tbd>.
  To create an appender, implement the <> and <> interfaces.

- A **logging pipeline** is a single-threaded process that reads log events from Kafka and invokes
  the appender defined in its configuration.

The *LogFramework* is configured using ``logback.xml`` files at a specified location. For every file
configured, a separate logging pipeline is created, providing isolation from other logging pipelines:

.. figure:: /_images/logging-framework.png
    :figwidth: 100%
    :width: 800px
    :align: center

    **CDAP Logging Framework:** Custom Log Pipeline and CDAP Log Pipeline, showing appenders

As indicated in the diagram above, each pipeline requires a unique name, which is used for
persisting the data and the retrieving of metadata. As they have separate Kafka consumers,
each pipeline has a different offset, and a slowly-processing pipeline doesn't affect the
performance of other logging pipelines.

Default Configuration
=====================
In the default configuration, CDAP uses <?>...

For many uses, this may be sufficient.

The default ``logback.xml`` file is located either in:

- For Standalone CDAP: ``<cdap-sdk-home>/conf/logback.xml``
- For Distributed CDAP: ``/opt/cdap/master/ext/logging/config``, as set by the property
  ``log.process.pipeline.config.dir`` in the ``cdap-default.xml`` file


Configuration Properties
------------------------
Configuration properties for logging pipelines and appenders are shown in the
documentation of the :ref:`logging properties <appendix-cdap-default-logging>` section of
the :ref:`cdap-site.xml <appendix-cdap-site.xml>` file.

In particular, these properties:

[Note: as all these props are doc'd at the above link, what subset of these would be appropriate? It doesn't seem to make sense to repeat all of them.]

- log.pipeline.cdap.dir.permissions
- log.pipeline.cdap.file.cleanup.interval.mins
- log.pipeline.cdap.file.cleanup.transaction.timeout
- log.pipeline.cdap.file.max.lifetime.ms
- log.pipeline.cdap.file.max.size.bytes
- log.pipeline.cdap.file.permissions
- log.pipeline.cdap.file.retention.duration.days
- log.pipeline.cdap.file.sync.interval.bytes

These properties control ...

- log.process.pipeline.auto.buffer.ratio
- log.process.pipeline.buffer.size
- log.process.pipeline.checkpoint.interval.ms
- log.process.pipeline.config.dir
- log.process.pipeline.event.delay.ms
- log.process.pipeline.kafka.fetch.size
- log.process.pipeline.lib.dir
- log.process.pipeline.logger.cache.expiration.ms
- log.process.pipeline.logger.cache.size

These properties (``log.process.pipeline.*``) can be specified at the pipeline level by
providing a value in a pipeline's ``logback.xml`` file for any of these properties.

These properties control ...

- log.publish.num.partitions
- log.publish.partition.key

Example Logback.xml File
------------------------

.. highlight:: xml

Here is an example ``logback.xml`` file, using two appenders (``STDOUT`` and
``rollingAppender``)::

  <?xml version="1.0" encoding="UTF-8"?>
  <configuration>
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
      <encoder>
        <pattern>%d{ISO8601} - %-5p [%t:%C{1}@%L] - %m%n</pattern>
      </encoder>
    </appender>

    <property name="cdap.log.saver.instance.id" value="instanceId"/>

    <appender name="rollingAppender" class="co.cask.cdap.logging.plugins.RollingLocationLogAppender">
  
      <!-- log file path will be created by the appender as: <basePath>/<namespace-id>/<application-id>/<filePath> -->
      <basePath>plugins/applogs</basePath>
      <filePath>securityLogs/logFile-${cdap.log.saver.instance.id}.log</filePath>
    
      <!-- cdap is the owner of the log files directory, so cdap will get read/write/execute permissions.
      Log files will be read-only for others. -->
      <dirPermissions>744</dirPermissions>
    
      <!-- cdap is the owner of the log files, so cdap will get read/write permissions.
      Log files will be read-only for others -->
      <filePermissions>644</filePermissions>

      <!-- It is an optional parameter, which takes number of miliseconds.
      Appender will close a file if it is not modified for fileMaxInactiveTimeMs
      period of time. Here it is set for thirty minutes. -->
      <fileMaxInactiveTimeMs>1800000</fileMaxInactiveTimeMs>

      <rollingPolicy class="co.cask.cdap.logging.plugins.FixedWindowRollingPolicy">
        <!-- Only specify the file name without a directory, as the appender will use the
        appropriate directory specified in filePath -->
        <fileNamePattern>logFile-${cdap.log.saver.instance.id}.log.%i</fileNamePattern>
        <minIndex>1</minIndex>
        <maxIndex>9</maxIndex>
      </rollingPolicy>

      <triggeringPolicy class="co.cask.cdap.logging.plugins.SizeBasedTriggeringPolicy">
        <!-- Set the maximum file size appropriately to avoid a large number of small files -->
        <maxFileSize>100MB</maxFileSize>
      </triggeringPolicy>

      <encoder>
        <pattern>%-4relative [%thread] %-5level %logger{35} - %msg%n</pattern>
        <!-- Do not flush on every event -->
        <immediateFlush>false</immediateFlush>
      </encoder>
    </appender>

    <logger name="co.cask.cdap.logging.plugins.RollingLocationLogAppenderTest" level="INFO">
      <appender-ref ref="rollingAppender"/>
    </logger>

    <root level="INFO">
      <appender-ref ref="STDOUT"/>
    </root>

  </configuration>


Custom Logging Pipeline
=======================
For a custom logging pipeline, you would create and configure a ``logback.xml`` file,
configuring loggers and appenders based on your requirements, and place the file at the
path specified by ``log.process.pipeline.config.dir``.

For every file configured, a separate logging pipeline is created. Though CDAP has been
tested with multiple logging pipelines and appenders, the fewer of each that are specified
will provide better performance.


Custom Appender
===============
If you need an appender beyond what is offered here, you can write and implement your own
custom appender. See the Logback documentation at
https://logback.qos.ch/manual/appenders.html for information on how to do this.

You can use any existing `logback <https://logback.qos.ch/manual/appenders.html>`__
appender. The ``RollingLocationLogAppender`` |---| an extension of the
``RollingFileLogAppender`` |---| lets you use HDFS locations in your logging pipelines. 

As the CDAP LogFramework uses the logback's Appender API, your custom appender needs to
implement the same Appender interface. Access to CDAP's system components (such as
datasets, metrics, ``LocationFactory``) are made available to the ``AppenderContext``, an
extension of logback's ``LoggerContext``.

Adding a dependency on the ``cdap-watch-dog`` API will allow you to access the
:cdap-java-source-github:`cdap-watchdog-api/src/main/java/co/cask/cdap/api/logging/AppenderContext.java`
in your application.






Log Location of CDAP System Services
====================================
The location of CDAP system service logs depends on the mode of CDAP and the Hadoop distribution.

- With **Standalone CDAP** (the CDAP SDK), system logs are located in ``<CDAP-SDK-HOME>/logs``.

- For **Distributed CDAP**, with the exception of Cloudera Manager, system logs are located in
  ``/var/log/cdap``. With Cloudera Manager installations, system log files are located in
  directories under ``/var/run/cloudera-scm-agent/process``.

These logs are emitted by the CDAP System Services:

- CDAP Master
- CDAP Router
- CDAP UI
- CDAP Auth Server
- CDAP Kafka Server


Application Logging Messages
============================

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
  
Application logs are stored in locations specified by properties in the
:ref:`cdap-site.xml <appendix-cdap-default-logging>` file:

- For CDAP Standalone: the property ``log.collection.root`` (default
  ``${local.data.dir}/logs``) is the root location for collecting logs when in Standalone
  CDAP.

- For Distributed CDAP: tbd




.. _master-service-logging-configuration:

Master Service Logging Configuration
====================================

:term:`Master Services <master services>` use ``logback-container.xml``, packaged with the CDAP distribution,
for logging configuration. This logback does log rotation once a day at midnight and expires logs older than
14 days. Changes can be made to ``logback-container.xml``; afterwards, the ``cdap-master``
process will need to be restarted for the modified logback to take effect.


Monitoring Utilities
====================
CDAP can be monitored using external systems such as `Nagios <https://www.nagios.org/>`__; a Nagios-style plugin 
`is available <https://github.com/caskdata/cdap-monitoring-tools/blob/develop/nagios/README.rst>`__
for checking the status of CDAP applications, programs, and the CDAP instance itself.

