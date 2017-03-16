.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

.. _logging-monitoring:

======================
Logging and Monitoring
======================

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

- to make logs persistent and available for later use and analysis;

- to be extensible using :ref:`custom log pipelines <logging-monitoring-custom-logging-pipeline>`; and

- to use a system that can be adapted for individual requirements at both the application
  and cluster levels.


Logging Example
===============
.. highlight:: console

This diagram shows the steps CDAP follows when logging a program of an application:

  - Logs are collected from an individual program running in a YARN container.

  - YARN containers have their own local log files.
  
  - CDAP logs are published to Kafka.

  - The Log Saver Service (*Log.saver*) is configured to read log messages from Kafka.
    Log.saver reads the messages from Kafka, buffers and sorts them in-memory, before
    persisting them to HDFS. These messages are written to files corresponding to the
    programs the log messages belong to.

  - Metrics are created based on the results of the pipeline, and passed to the Metrics Service.
   
  - For security, the files written out to persistent storage in HDFS have permissions set
    such that they are accessible only by the ``cdap`` user.

  - Log.saver also emits metrics for each *info, debug,* or *error* message received. This
    helps in monitoring the amount of logs emitted by applications, application's programs,
    system services, etc. and can be retrieved by querying the :ref:`CDAP metrics system
    <http-restful-api-metrics>`.

.. figure:: /_images/logging/logging-framework-simple.png
    :figwidth: 100%
    :width: 800px
    :align: center

    **CDAP Logging Example:** From a YARN container, through Kafka and the Log Saver Service, to HDFS

Logging uses the standard `SLF4J (Simple Logging Facade for Java)
<http://www.slf4j.org/manual.html>`__ APIs and `Logback
<https://logback.qos.ch/manual>`__. Logging is configured using instances of Logback's
"logback" file, consisting of *logging pipelines* with *logging appenders*:

  - A **logging pipeline** is a process that consumes log events from Kafka, invokes
    *logback appenders* defined in its configuration, buffering, sorting, and then
    persisting the log messages.

  - A **logback appender** (or *appender*) is a Java class, responsible for writing the
    log events to persistent storage and maintaining metadata about that storage.


User Application Program Logs
=============================

Emitting Log Messages from a Program
------------------------------------
.. highlight:: java

CDAP supports application logging through the standard `SLF4J (Simple Logging Facade for Java)
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

This will emit "info" level messages from a flowlet when it is processing an event.

Retrieving Log Messages from a Program
--------------------------------------
The log messages emitted by your application code can be retrieved by:

- Using the :ref:`restful-api`: the :ref:`Logging HTTP RESTful API
  <http-restful-api-logging>` details the available contexts that can be called to
  retrieve different messages.
- Log messages of a program can be viewed in the :ref:`CDAP UI <cdap-ui>`.

Program Log File Locations
--------------------------
Program logs are stored in locations specified by properties in the
:ref:`cdap-site.xml <appendix-cdap-default-logging>` file depending on the mode 
of CDAP (Standalone or Distributed):

- For **Standalone CDAP:** the property ``log.collection.root`` (default
  ``${local.data.dir}/logs``) is the root location for collecting program logs when in
  Standalone CDAP.

- For **Distributed CDAP:** the property ``log.base.dir`` (default ``/logs/avro``) is the
  base directory in HDFS for program logs

.. _configuring-user-application-program-logging:

Configuring Program Logs and Log Levels
---------------------------------------
The logging of an application's programs are configured by the ``logback-container.xml``
file, packaged with the CDAP distribution. This "logback" does log rotation once a day at
midnight and expires logs older than 14 days. Changes can be made to
``logback-container.xml``; afterwards, the ``cdap-master`` process will need to be
restarted for the modified logback file to take effect.

- For **Distributed CDAP:** the ``logback-container.xml`` file is located in ``/etc/cdap/conf``.
- For **Standalone CDAP:** the file ``logback.xml`` file, located in  ``<cdap-sdk-home>/conf``,
  configures both "container" and CDAP system service logging.
  
You can also use a custom "logback" file with your application, as described in the
Developers' Manual section :ref:`application-logback`.

Changing Program Log Levels
---------------------------
The CDAP :ref:`Logging HTTP RESTful API
<http-restful-api-logging-changing-program-log-levels>` can be used to set the log levels
for a particular run of a program. Once changed, they can be reset back to what they
started with by using the :ref:`reset endpoint <http-restful-api-logging-resetting>`.

To change the log level before an application starts, you can add the logger name as the
key and log level as the value in the preferences. The logger name should be prefixed with
``system.log.level``. 

For example, if you want to change the log level of the ``HelloWorld`` class of the
:ref:`Hello World example <examples-hello-world>` to ``DEBUG``, you would use
``system.log.level.co.cask.cdap.examples.helloworld.HelloWorld`` as the key and ``DEBUG``
as the value. This can be applied to any package or classes. If the logger name is
omitted, it will change the log level of ROOT.

To change the log level of a program dynamically (for a worker, flow, or service which are
running), see :ref:`Logging HTTP RESTful API
<http-restful-api-logging-changing-program-log-levels>`. You can change the log level at
run level of a worker, flow, or service.

**Note:** The log levels can only be changed for programs that are running under
Distributed CDAP. Changing the log levels for programs in the CDAP Standalone is not
supported. You have to either modify the ``logback.xml`` or :ref:`provide
<application-logback>` a ``logback.xml`` for your application. 


.. _logging-monitoring-system-services-logs:

CDAP System Services Logs
=========================
As CDAP system services run either on cluster edge nodes or in YARN containers, their
logging and its configuration depends on the service and where it is located:

- If run on an edge node, such as CDAP Master or Router, it is configured by the
  file ``logback.xml``.

- If run in a YARN container, such as the *Metrics Service*, it is configured by the file
  ``logback-container.xml`` (the same file that :ref:`configures user application program logging
  <configuring-user-application-program-logging>`).

Retrieving Log Messages from a System Service
---------------------------------------------
The log messages emitted by CDAP system services can be retrieved by:

- Using the :ref:`restful-api`: the :ref:`Logging HTTP RESTful API
  <http-restful-api-logging>` details :ref:`downloading the logs
  <http-restful-api-logging-downloading-system-logs>` emitted by a system service.
- Log messages of system services can be viewed in the :ref:`CDAP UI <cdap-ui>`.

System Service Log File Locations
---------------------------------
The location of CDAP system service logs depends on the mode of CDAP (Standalone or
Distributed) and the Hadoop distribution:

- For **Standalone CDAP:** system logs are located in ``<CDAP-SDK-HOME>/logs``.

- For **Distributed CDAP:** system logs are located in ``/var/log/cdap`` (with the
  exception of Cloudera Manager-based clusters). With Cloudera Manager installations, system
  log files are located in directories under ``/var/run/cloudera-scm-agent/process``.

Configuring System Service Logs
-------------------------------
- To configure logging by CDAP system services that run in YARN containers, see the
  section on :ref:`configuring user application program logging
  <configuring-user-application-program-logging>`.

- Logging by CDAP system services that run on cluster edge nodes is configured by the
  ``logback.xml`` Changes can be made to ``logback.xml``; afterwards, the service(s)
  affected will need to be restarted for the modified "logback" file to take effect.

  - For **Standalone CDAP:** the ``logback.xml`` file is located in ``/etc/cdap/conf``.
  - For **Distributed CDAP:** the file ``logback.xml`` file, located in 
    ``<cdap-sdk-home>/conf``, configures both "container" and CDAP system service logging.


Configuring the Log Saver Service
=================================
The Log Saver Service is the CDAP service that reads log messages from Kafka, processes
them in *log pipelines*, persists them to HDFS, and sends metrics on logging to the
Metrics Service.

Properties and configurations that control the Log Saver Service are located in the
:ref:`cdap-site.xml <appendix-cdap-site.xml>` file.

CDAP Site XML File
------------------
The :ref:`cdap-site.xml <appendix-cdap-site.xml>` file has properties that control the
writing of logs to Kafka, the CDAP Log Saver Service, the CDAP log pipeline, and any
:ref:`custom log pipelines <logging-monitoring-custom-logging-pipeline>` that have been defined.

Writing Logs to Kafka
.....................
These :ref:`properties <appendix-cdap-default-logging>` control the writing of logs to
Kafka:

.. generated by code in conf.py for this manual
.. include:: /../target/_includes/logging-writing-logs-to-kafka.rst

Log Saver Service
.................
These :ref:`properties <appendix-cdap-default-logging>` control the Log Saver Service:

.. generated by code in conf.py for this manual
.. include:: /../target/_includes/logging-log-saver-service.rst

Log saver instances should be from a minimum of one to a maximum of ten. The maximum is
set by the number of Kafka partitions (``log.publish.num.partitions``), which by default is 10.

.. _logging-monitoring-logging-pipeline-configuration:

Logging Pipeline Configuration
..............................
Configuration properties for logging and :ref:`custom logging pipelines
<logging-monitoring-custom-logging-pipeline>` are shown in the documentation of the
:ref:`logging properties <appendix-cdap-default-logging>` section of the
:ref:`cdap-site.xml <appendix-cdap-site.xml>` file.

The **CDAP log pipeline** is configured by settings in the :ref:`cdap-site.xml
<appendix-cdap-site.xml>` file.

**Custom logging pipelines** are configured by a combination of the settings in the 
:ref:`cdap-site.xml <appendix-cdap-site.xml>` file and a "logback" file used
to specify the custom pipeline. The XML file is placed in the
``log.process.pipeline.config.dir``, a local directory on the CDAP Master node that is scanned
for log processing pipeline configurations. Each pipeline is defined by a file in the
Logback XML format, with ``.xml`` as the file name extension.

These :ref:`properties <appendix-cdap-default-logging>` control the CDAP log pipeline:

.. generated by code in conf.py for this manual
.. include:: /../target/_includes/logging-pipeline-configuration-1.rst

.. _custom-log-pipeline-logging-configuration:

These :ref:`properties <appendix-cdap-default-logging>` control both the CDAP log pipeline
and custom log pipelines:

.. generated by code in conf.py for this manual
.. include:: /../target/_includes/logging-pipeline-configuration-2.rst

The ``log.process.pipeline.*`` properties can be over-ridden and specified at the
custom pipeline level by providing a value in a pipeline's "logback" file for any of
these properties.


Logging Framework
=================

.. highlight:: console

This diagram shows in greater detail the components and steps CDAP follows when logging
programs of an application and system services with the logging framework:

  - Logs are collected from individual programs running in YARN containers.

  - YARN containers have their own local log files.

  - CDAP System Services run (depending on the service) either on cluster edge nodes or in
    YARN containers. Where they run determines the file that configures that service's
    logging.

  - CDAP logs are published to Kafka topics.

  - The Log Saver Service (*Log.saver*) is configured to read log messages for the
    ``logs.user-v2`` Kafka topic. The number of log saver instances can be scaled to process
    the Kafka partitions in parallel, if needed. Log.saver reads the messages from Kafka,
    buffers and sorts them in-memory, before persisting them to HDFS. These messages are
    written to files corresponding to the programs the log messages belong to.

    *Note:* These files are configured to rotate based on time and size; they can be
    changed using the properties ``log.pipeline.cdap.file.max.size.bytes`` and
    ``log.pipeline.cdap.file.max.lifetime.ms`` in the :ref:`cdap-site.xml
    <appendix-cdap-default-logging>` file as described in 
    :ref:`logging-monitoring-logging-pipeline-configuration`.

  - Custom pipelines can be configured by adding an XML file in a prescribed location.
    Each pipeline buffers log messages in-memory and sorts them based on their timestamp. 
   
  - Metrics are created based on the results of the pipelines, and passed to the Metrics
    Service.
   
  - For security, the files written out to persistent storage in HDFS have permissions set
    such that they are accessible only by the ``cdap`` user.

  - Log.saver also emits metrics for each *info, debug,* or *error* message received. This
    helps in monitoring the amount of logs emitted by applications, application's programs,
    system services, etc. and can be retrieved by querying the :ref:`CDAP metrics system
    <http-restful-api-metrics>`.

.. figure:: /_images/logging/logging-framework.png
    :figwidth: 100%
    :width: 800px
    :align: center

    **CDAP Logging Framework:** From YARN containers, through Kafka and the Log Saver
    Service, to HDFS

Logging Standards
-----------------
Logging uses the standard `SLF4J (Simple Logging Facade for Java)
<http://www.slf4j.org/manual.html>`__ APIs and `Logback
<https://logback.qos.ch/manual>`__. Logging is configured using instances of Logback's
"logback" file, consisting of *logging pipelines* with *logging appenders*:

  - A **logging pipeline** is a process that consumes log events from Kafka, invokes
    *logback appenders* defined in its configuration, buffering, sorting, and then
    persisting the log messages.

  - A **logback appender** (or *appender*) is a Java class, responsible for writing the
    log events to persistent storage and maintaining metadata about that storage.

Configuration Files
-------------------
- The ``logback-container.xml`` configures the logs of YARN containers, both locally and to Kafka,
  for application programs and any :ref:`CDAP System Services
  <logging-monitoring-system-services-logs>` (such as the *Metrics Service*) that run in YARN. 
- The ``logback.xml`` configures the edge node-located :ref:`CDAP System Services
  <logging-monitoring-system-services-logs>`, such as the *App Fabric* or *Data Service*.
- Both of these files are, in Distributed CDAP, located in ``/etc/cdap/conf``.
- In the case of :ref:`Standalone CDAP <modes-data-application-platform>`, as containers
  are replaced with Java threads, only a single file (``logback.xml``, located in
  ``<cdap-sdk-home>/conf``) is used, replacing the ``logback.xml`` and
  ``logback-container.xml`` of Distributed CDAP.
- The ``cdap-site.xml`` controls the CDAP Log Pipeline for CDAP System Services, and the
  reading, buffering, and memory allocated.
- Custom "logback" files, if present in a specified directory, trigger the creation of
  custom logging pipelines, with a pipeline created for each custom file.

.. _logging-monitoring-custom-logging-pipeline:

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

For every XML file in the ``log.process.pipeline.config.dir`` directory, a separate logging pipeline is created, providing
isolation from other logging pipelines. As they are separate Kafka consumers and
processes, each pipeline is independent of |---| and doesn't affect the performance of
|---| other logging pipelines. Though CDAP has been tested with multiple logging pipelines
and appenders, the fewer of each that are specified will provide better performance.

.. _logging-monitoring-custom-logging-configuration:

Custom Logging Pipeline Configuration Pipelines
-----------------------------------------------
CDAP looks for Logback "logback" files located in a directory as set by the property
``log.process.pipeline.config.dir`` in the :ref:`cdap-site.xml
<appendix-cdap-default-logging>` file. In the default configuration, this is:
  
- For **Standalone CDAP:** ``<cdap-sdk-home>/ext/logging/config``
- For **Distributed CDAP:** ``/opt/cdap/master/ext/logging/config``

.. _logging-monitoring-custom-logging-example:

Example "logback" File for a Custom Logging Pipeline
----------------------------------------------------
.. highlight:: xml

Here is an example "logback" file, using two appenders (``STDOUT`` and
``rollingAppender``). It would be saved with the file extension ``.xml`` to a location as
noted :ref:`above <logging-monitoring-custom-logging-configuration>`::

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

Custom Logging Appender
-----------------------
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
<reference:java-client-api>`, and the :ref:`application-logback`.

