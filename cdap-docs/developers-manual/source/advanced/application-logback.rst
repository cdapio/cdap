.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015-2017 Cask Data, Inc.

.. _logging-framework:

======================
CDAP Logging Framework
======================

.. highlight:: console

CDAP collects logs of both its internal services and user applications. To do so, CDAP
uses a logging framework, based on `Logback <https://logback.qos.ch/manual>`__, consisting
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

log.pipeline.cdap.dir.permissions
log.pipeline.cdap.file.cleanup.interval.mins
log.pipeline.cdap.file.cleanup.transaction.timeout
log.pipeline.cdap.file.max.lifetime.ms
log.pipeline.cdap.file.max.size.bytes
log.pipeline.cdap.file.permissions
log.pipeline.cdap.file.retention.duration.days
log.pipeline.cdap.file.sync.interval.bytes


log.process.pipeline.auto.buffer.ratio
log.process.pipeline.buffer.size
log.process.pipeline.checkpoint.interval.ms
log.process.pipeline.config.dir
log.process.pipeline.event.delay.ms
log.process.pipeline.kafka.fetch.size
log.process.pipeline.lib.dir
log.process.pipeline.logger.cache.expiration.ms
log.process.pipeline.logger.cache.size

log.publish.num.partitions
log.publish.partition.key

These properties (``log.process.pipeline.*``) can be specified at the pipeline level by
providing a value in a pipeline's ``logback.xml`` file for any of these properties.

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


.. _application-logback:

Application Logback
===================

YARN containers launched by a CDAP application use a default container logback file
|---| ``logback-container.xml`` |---| packaged with CDAP and installed in 
the CDAP :ref:`configuration directory <admin-manual-cdap-components>`. This logback does
log rotation once every day at midnight and deletes logs older than 14 days. Depending on
the use case, the default configuration may be sufficient. As long as the container is
running, these policies will apply. (As the lifetime of many containers is often less than
14 days, these limits may never be reached.)

**Note:** In the case of the CDAP Standalone, the logback file used is ``logback.xml``, located
in the ``<cdap-sdk-home>/conf`` directory.

You can specify a custom ``logback.xml`` for a CDAP application by packaging
it with the application in the application's ``src/main/resources`` directory.
The packaged ``logback.xml`` is then used for each container launched by the application.

To write a custom ``logback.xml``, refer to the `Logback manual <http://logback.qos.ch/manual>`__
for information.

**Note:** When a custom ``logback.xml`` is specified for an application, the custom
``logback.xml`` will be used in place of the ``logback-container.xml``. A custom
``logback.xml`` needs to be configured for log rotation (``rollingPolicy``) and log
clean-up (``maxHistory``) to ensure that long-running containers don't fill up the disk.
