.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

.. _logging-monitoring:

======================
Logging and Monitoring
======================

Logs and Metrics
================
CDAP collects logs and metrics for all of its internal services and on user applications.
Being able to view these details can be very helpful in debugging CDAP applications as
well as analyzing their performance. CDAP gives access to its logs, metrics, and other
monitoring information through RESTful APIs, the CDAP UI, as well as a Java Client.

In Hadoop clusters, the programs running in containers generate individual log files as
part of the container. As these files generally do not persist beyond the life of the
container, they are volatile and not very useful for post-mortem diagnostics and
troubleshooting, or for analyzing performance. As a single application can consist of
multiple  programs distributed across the nodes of a cluster, the complete logs for the
application may similarly be distributed.

To address these issues, the CDAP log framework was designed:

- to centralize the location of logs, so that the individual logs of an application's programs can be
  merged into one;

- to make logs persistent and available for later use and analysis; and

- to use a system that can be adapted for individual requirements at both the application
  and cluster levels.


Logging Framework
=================

.. highlight:: console

This diagram shows the steps CDAP follows with its logging framework:

  1. Logs are collected from individual programs running in YARN containers.

  #. YARN containers have their own local log files.

  #. CDAP logs are published to Kafka topics.

  #. The Log Saver Service (*Log.saver*) is configured to read log messages for the ``logs.user-v2``
     Kafka topic. The number of log saver instances can be scaled to process the Kafka
     partitions in parallel, if needed. Log.saver reads the messages from Kafka, buffers
     and sorts them in-memory, before persisting them to HDFS. These messages are written
     to files corresponding to the programs the log messages belong to.

     *Note:* These files are configured to rotate based on time and size; they can be
     changed using the properties ``log.pipeline.cdap.file.max.size.bytes`` and
     ``log.pipeline.cdap.file.max.lifetime.ms`` in the :ref:`cdap-site.xml
     <appendix-cdap-default-logging>` file as described in 
     :ref:`logging-monitoring-logging-pipeline-configuration`.

  #. Custom pipelines can be configured by adding an XML file in a prescribed location.
     Each pipeline buffers log messages in-memory and sorts them based on their timestamp. 
   
  #. Metrics are created based on the results of the pipelines, and passed to the Metrics Service.
   
  #. Logs are written out to persistent storage in HDFS by the logging pipelines. For
     security, the files are written with permissions set such that they are accessible only
     by the ``cdap`` user.

  #. Log.saver also emits metrics for each *info, debug,* or *error* message received. This
     helps in monitoring the amount of logs emitted by applications, application's programs,
     system services, etc. by querying the :ref:`CDAP metrics system <http-restful-api-metrics>`.

.. figure:: /_images/logging-framework.png
    :figwidth: 100%
    :width: 800px
    :align: center

    **CDAP Logging Framework:** From YARN containers, through Kafka and the Log Saver Service, to HDFS

Logging uses standard `SLF4J (Simple Logging Facade for Java)
<http://www.slf4j.org/manual.html>`__ APIs and `Logback
<https://logback.qos.ch/manual>`__, and consists of *logging pipelines* with *appenders*:

  - A **logging pipeline** is a process that consumes log events from Kafka, invokes
    *logback appenders* defined in its configuration, buffering, sorting, and then
    persisting the log messages.

  - A **logback appender** (or *appender*) is a Java class, responsible for writing the
    log events to persistent storage and maintaining metadata about that storage.

Logging is configured using Logback's ``logback.xml`` files:

- The ``logback-container.xml`` controls the program containers, both locally and to Kafka.
- The ``logback.xml`` controls the :ref:`CDAP System Services logs <logging-monitoring-system-services-logs>`.
- Both of these files are, in Distributed CDAP, located in ``/etc/cdap/conf``.
- In the case of :ref:`Standalone CDAP <modes-data-application-platform>`, as containers
  are replaced with Java threads, only a single file (``logback.xml``, located in
  ``<cdap-sdk-home>/conf``) is used, replacing the ``logback.xml`` and
  ``logback-container.xml`` of Distributed CDAP.
- The ``cdap-site.xml`` controls the CDAP Log Pipeline for CDAP System Services, and the
  reading, buffering, and memory allocated.
- Custom XML files, if present in a specified directory, trigger the creation of custom pipelines, with
  a pipeline created for each custom file.


Log Locations
=============

.. _logging-monitoring-system-services-logs:

CDAP System Services Logs
-------------------------
These logs are emitted by the CDAP System Services:

- CDAP Master
- CDAP Router
- CDAP UI
- CDAP Auth Server
- CDAP Kafka Server

The location of CDAP system service logs depends on the mode of CDAP and the Hadoop distribution:

- For **Standalone CDAP:** system logs are located in ``<CDAP-SDK-HOME>/logs``.

- For **Distributed CDAP:** system logs are located in ``/var/log/cdap`` (with the
  exception of Cloudera Manager-based clusters). With Cloudera Manager installations, system
  log files are located in directories under ``/var/run/cloudera-scm-agent/process``.

Application Logs
----------------
.. highlight:: java

CDAP supports application logging through standard `SLF4J (Simple Logging Facade for Java)
<http://www.slf4j.org/manual.html>`__ APIs.

For instance, in a flowlet you can write::

  private static Logger LOG = LoggerFactory.getLogger(WordCounter.class);
  ...
  @ProcessInput
  public void process(String line) {
    LOG.info("{}: Received line {}", this.getContext().getTransactionAwareName(), line);
    ... // processing
    LOG.info("{}: Emitting count {}", this.getContext().getTransactionAwareName(), wordCount);
    output.emit(wordCount);
  }

The log messages emitted by your application code can be retrieved by:

- Using the :ref:`restful-api`. The :ref:`Logging HTTP RESTful API <http-restful-api-logging>` 
  details the available contexts that can be called to retrieve different messages.
- Log messages of a program can be viewed in the :ref:`CDAP UI <cdap-ui>`.
  
Application logs are stored in locations specified by properties in the
:ref:`cdap-site.xml <appendix-cdap-default-logging>` file:

- For **Standalone CDAP:** the property ``log.collection.root`` (default
  ``${local.data.dir}/logs``) is the root location for collecting logs when in Standalone
  CDAP.

- For **Distributed CDAP:** the property ``log.base.dir`` (default ``/logs/avro``) is the
  base directory in HDFS for logs
  

Configuring Logging
===================
Properties and configurations that control logging are located in Logback XML files and in
the :ref:`cdap-site.xml <appendix-cdap-site.xml>` file.

In the default configuration, CDAP uses Logback XML files located:

- For Standalone CDAP: ``<cdap-sdk-home>/conf/logback.xml``
- For Distributed CDAP: ``/opt/cdap/master/ext/logging/config``, as set by the property
  ``log.process.pipeline.config.dir`` in the ``cdap-default.xml`` file

.. _master-service-logging-configuration:

Master Service Logging Configuration
------------------------------------
:term:`Master Services <master services>` use the ``logback-container.xml``, packaged with
the CDAP distribution, as the logging configuration. This logback does log rotation once a
day at midnight and expires logs older than 14 days. Changes can be made to
``logback-container.xml``; afterwards, the ``cdap-master`` process will need to be
restarted for the modified logback to take effect.

CDAP Site XML File
------------------
The :ref:`cdap-site.xml <appendix-cdap-site.xml>` file has properties that control the
writing of logs to Kafka, the CDAP Log Saver Service, the CDAP log pipeline, and any
custom log pipelines that have been defined.

Writing Logs to Kafka
.....................
These properties control the writing of logs to Kafka:

- ``log.publish.num.partitions`` (default 10)
- ``log.publish.partition.key`` (default ``program``) Publish logs from an application or
  a program to the same partition. Valid values are 'application' or 'program'. If set to
  'application', logs from all the programs of an application go to the same partition. If
  set to 'program', logs from the same program go to the same partition. Changes to this
  property requires restarting of all CDAP applications.

Log Saver Service
.................
These properties control the Log Saver Service:

- ``log.saver.max.instances`` (default ``${master.service.max.instances}``)
- ``log.saver.num.instances`` (default 1)
- ``log.saver.container.memory.mb``
- ``log.saver.container.num.cores``
- ``log.saver.status.bind.address``

Log saver instances should be from a minimum of one to a maximum of ten. The maximum is
set by the number of Kafka partitions (``log.publish.num.partitions``), in the default instance 10.

.. _logging-monitoring-logging-pipeline-configuration:

Logging Pipeline Configuration
..............................
Configuration properties for logging and custom logging pipelines are shown in the
documentation of the :ref:`logging properties <appendix-cdap-default-logging>` section of
the :ref:`cdap-site.xml <appendix-cdap-site.xml>` file.

The **CDAP log pipeline** is configured by settings in the :ref:`cdap-site.xml
<appendix-cdap-site.xml>` file.

**Custom logging pipelines** are configured by a combination of the settings in the 
:ref:`cdap-site.xml <appendix-cdap-site.xml>` file and the Logback XML file used
to specify the custom pipeline. The XML file is placed in the
``log.process.pipeline.config.dir``, a local directory on the CDAP Master that is scanned
for log processing pipeline configurations. Each pipeline is defined by a file in the
Logback XML format, with ``.xml`` as the file name extension.

These properties control the CDAP log pipeline:

- ``log.pipeline.cdap.dir.permissions``
- ``log.pipeline.cdap.file.cleanup.interval.mins``
- ``log.pipeline.cdap.file.cleanup.transaction.timeout``
- ``log.pipeline.cdap.file.max.lifetime.ms``
- ``log.pipeline.cdap.file.max.size.bytes``
- ``log.pipeline.cdap.file.permissions``
- ``log.pipeline.cdap.file.retention.duration.days``
- ``log.pipeline.cdap.file.sync.interval.bytes``

.. _custom-log-pipeline-logging-configuration:

These properties control both the CDAP log pipeline and custom log pipelines:

- ``log.process.pipeline.auto.buffer.ratio``
- ``log.process.pipeline.buffer.size``
- ``log.process.pipeline.checkpoint.interval.ms``
- ``log.process.pipeline.config.dir``
- ``log.process.pipeline.event.delay.ms``
- ``log.process.pipeline.kafka.fetch.size``
- ``log.process.pipeline.lib.dir``
- ``log.process.pipeline.logger.cache.expiration.ms``
- ``log.process.pipeline.logger.cache.size``

The ``log.process.pipeline.*`` properties can be over-ridden and specified at the
custom pipeline level by providing a value in a pipeline's Logback XML file for any of
these properties.


Custom Logging Pipeline
=======================
For a custom logging pipeline, you would create and configure a Logback XML file,
configuring loggers, appenders, and properties based on your requirements, and place the
file at the path specified in the ``cdap-site.xml`` file by the property
``log.process.pipeline.config.dir`` of the ``cdap-site.xml`` file.

Each custom pipeline requires a unique name, which is used for persisting the data and the
retrieving of metadata. Properties controlling the pipeline (the
``log.process.pipeline.*`` properties) are described :ref:`above
<custom-log-pipeline-logging-configuration>`.

For every XML file in the directory, a separate logging pipeline is created, providing
isolation from other logging pipelines. As they are separate Kafka consumers and
processes, each pipeline is independent of |---| and doesn't affect the performance of
|---| other logging pipelines. Though CDAP has been tested with multiple logging pipelines
and appenders, the fewer of each that are specified will provide better performance.

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


Custom Appender
===============
If you need an appender beyond what is available through Logback or CDAP, you can write
and implement your own custom appender. See the `Logback documentation
<https://logback.qos.ch/manual/appenders.html>`__ for information on this.

You can use any existing `logback <https://logback.qos.ch/manual/appenders.html>`__
appender. The ``RollingLocationLogAppender`` |---| an extension of the Logback
``FileAppender`` |---| lets you use HDFS locations in your logging pipelines. 

As the CDAP LogFramework uses Logback's Appender API, your custom appender needs to
implement the same Appender interface. Access to CDAP's system components (such as
datasets, metrics, ``LocationFactory``) are made available to the ``AppenderContext``, an
extension of Logback's ``LoggerContext``.

Adding a dependency on the ``cdap-watch-dog`` API will allow you to access the
:cdap-java-source-github:`cdap-watchdog-api/src/main/java/co/cask/cdap/api/logging/AppenderContext.java`
in your appender.


Monitoring Utilities
====================
CDAP can be monitored using external systems such as `Nagios <https://www.nagios.org/>`__;
a Nagios-style plugin `is available
<https://github.com/caskdata/cdap-monitoring-tools/blob/develop/nagios/README.rst>`__ for
checking the status of CDAP applications, programs, and the CDAP instance itself.


Additional References
=====================
For additional information beyond here, see the :ref:`Logging <http-restful-api-logging>`,
:ref:`Metrics <http-restful-api-metrics>`, and :ref:`Monitoring
<http-restful-api-monitor>` HTTP RESTful APIs, the :ref:`Java Client
<reference:java-client-api>`, :ref:`master-service-logging-configuration`, and the 
:ref:`application-logback`.

