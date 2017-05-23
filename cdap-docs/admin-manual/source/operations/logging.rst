.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

.. _logging-monitoring:

======================
Logging and Monitoring
======================

CDAP collects logs and metrics for all of its internal services and user applications.
Being able to view these details can be very helpful in debugging CDAP applications as
well as analyzing their performance. CDAP gives access to its logs, metrics, and other
monitoring information through RESTful APIs, the CDAP UI, as well as a Java Client.

In Hadoop clusters, the programs running inside their containers generate individual log files as
part of the container. 
As an application can consist of
multiple  programs distributed across the nodes of a cluster, the complete logs for the
application may similarly be distributed.
As these files generally do not persist beyond the life of the
container, they are volatile and not very useful for post-mortem diagnostics and
troubleshooting, or for analyzing performance. 

To address these issues, the CDAP log framework was designed:

- to centralize the location of logs, so that the logs of the individual containers of a
  program can be merged into one;

- to make logs both persistent |---| available for later use and analysis |---| and
  available while the program is still running;

- to be extensible using :ref:`custom log pipelines <logging-monitoring-configuring-custom-log-pipelines>`; and

- to allow fine-tuning of the logging behavior at the level of an individual application
  as well as the entire cluster.


Logging Example
===============
.. highlight:: console

This diagram shows the steps CDAP follows when logging a program of an application:

  - Logs are collected from an individual program running in a YARN container.

  - YARN writes the log messages emitted by containers to files inside the container.
  
  - In addition, CDAP programs publish these messages to Kafka.

  - The CDAP Log Saver Service is configured to read log messages from Kafka.
    Log saver reads the messages from Kafka, groups them by program or application,
    buffers and sorts them in memory, and finally persists them to files in HDFS. Each of
    these files corresponds to one program or application, depending on how the grouping
    is configured. (This is set by the property :ref:`log.publish.partition.key
    <logging-monitoring-writing-logs-to-kafka>`, described :ref:`below
    <logging-monitoring-configuring-the-log-saver-service>`.)

  - In addition to persisting logs to files, the Log.saver also emits metrics about the
    number of log messages emitted by each program. These metrics can be retrieved by
    querying the :ref:`CDAP metrics system <http-restful-api-metrics>`.
   
  - For security, the files written out to persistent storage in HDFS have permissions set
    such that they are accessible only by the ``cdap`` user.

.. figure:: /_images/logging/logging-framework-simple.png
    :figwidth: 100%
    :width: 800px
    :align: center

    **CDAP Logging Example:** From a YARN container, through Kafka and the CDAP Log Saver Service, to HDFS

Logging uses the standard `SLF4J (Simple Logging Facade for Java)
<http://www.slf4j.org/manual.html>`__ APIs and `Logback
<https://logback.qos.ch/manual>`__. Logging is configured using instances of Logback's
"logback" file, consisting of *log pipelines* with *log appenders*:

  - A **log pipeline** is a process that consumes log events from Kafka, buffers, groups
    by application or program, sorts, and then invokes the *log appenders* defined in its
    configuration.

  - A **log appender** (or *appender*) is a Java class, responsible for consuming and
    processing messages; typically, this includes persisting the log events. It can also,
    for example, collect metrics, maintain metadata about the storage, or emit alerts when
    it finds certain messages.


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
    LOG.debug("{}: Received line {}", this.getContext().getTransactionAwareName(), line);
    ... // processing
    LOG.debug("{}: Emitting count {}", this.getContext().getTransactionAwareName(), wordCount);
    output.emit(wordCount);
  }

This will emit "debug" level messages from the flowlet when it is processing an event.

Retrieving Log Messages from a Program
--------------------------------------
The log messages emitted by your application code can be retrieved by:

- Using the :ref:`restful-api`: the :ref:`Logging HTTP RESTful API
  <http-restful-api-logging>` details the available contexts that can be called to
  retrieve different messages.
- Log messages of a program can be viewed in the :ref:`CDAP UI <cdap-ui>`. In the
  *Overview* page, select the application and program that you are interested in, and click
  its *Logs* icon (circled in red here):

  .. figure:: /_images/logging/logging-cdap-ui-purchase.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image

      **CDAP UI:** *Log* icon, enabled if there are logs available for viewing 

  The logs will be displayed in the log viewer:

  .. figure:: /_images/logging/logging-cdap-ui-purchase-flow-log.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image

      **CDAP UI:** *Log Viewer*, showing *PurchaseFlow* log events, *INFO* level

Program Log File Locations
--------------------------
Program logs are stored in locations specified by properties in the
:ref:`cdap-site.xml <appendix-cdap-default-logging>` file depending on the mode 
of CDAP (Standalone or Distributed):

- For **Standalone CDAP:** the property ``log.collection.root`` (default
  ``${local.data.dir}/logs``) is the root location for collecting program logs when in
  Standalone CDAP.

- For **Distributed CDAP:** the property ``hdfs.namespace`` (default ``/cdap``) is the
  base directory in HDFS; program logs are stored in ``${hdfs.namespace}/logs`` (by default, 
  ``/cdap/logs``).

.. _configuring-user-application-program-logging:

Configuring Program Logs and Log Levels
---------------------------------------
The logging of an application's programs are configured by the ``logback-container.xml``
file, packaged with the CDAP distribution. This "logback" does log rotation once a day at
midnight and expires logs older than 14 days. Changes can be made to
``logback-container.xml``; afterwards, applications or programs needs to be restarted
for the modified logback file to take effect. Changing the ``logback-container.xml`` will
only affect programs that are started after the change; existing running programs will not
be affected.

- For **Standalone CDAP:** As the entire Standalone CDAP runs in a single JVM, the
  ``logback.xml`` file, located in  ``<cdap-sdk-home>/conf``, configures both "container"
  and CDAP system service logging.
- For **Distributed CDAP:** the ``logback-container.xml`` file is located in ``/etc/cdap/conf``.

You can also use a custom "logback" file with your application, as described in the
Developers' Manual section :ref:`application-logback`.

Changing Program Log Levels
---------------------------
When running under Distributed CDAP, the log levels of a program can be changed without
modifying the ``logback.xml`` or ``logback-container.xml`` files. This can be done, for
all program types, before a particular run or |---| in the case of a flow, service, or
worker |---| while it is running.

The CDAP :ref:`Logging HTTP RESTful API
<http-restful-api-logging-changing-program-log-levels>` can be used to set the log levels
of a flow, service, or worker which is running. Once changed, they can be reset back to what they
were originally by using the :ref:`reset endpoint <http-restful-api-program-logging-resetting>`.

Only flows, services, or workers can be dynamically changed; other program types are
currently not supported. The other program types' log levels can only be changed using
their preferences before the program starts.

- To configure the log level before an application starts, you can add the logger name as the
  key and log level as the value in the :ref:`preferences <http-restful-api-preferences>`,
  using the :ref:`CDAP UI <cdap-ui>`, :ref:`CDAP CLI <cdap-cli>`, or other command line
  tools. The logger name should be prefixed with ``system.log.level``. 

  For example, if you want to set the log level of the ``HelloWorld`` class of the
  :ref:`Hello World example <examples-hello-world>` to ``DEBUG``, you would use
  ``system.log.level.co.cask.cdap.examples.helloworld.HelloWorld`` as the key and ``DEBUG``
  as the value. This can be applied to any package or classes. If the logger name is
  omitted, it will change the log level of ROOT.

- To configure the log level of a program dynamically |---| such as a flow, service, or worker
  which is currently running |---| see the :ref:`Logging HTTP RESTful API
  <http-restful-api-logging-changing-program-log-levels>`.

**Note:** The :ref:`Logging HTTP RESTful API
<http-restful-api-logging-changing-program-log-levels>` for changing program log levels
can only be used with programs that are running under Distributed CDAP. For changing the log
levels of programs run under Standalone CDAP, you either modify the ``logback.xml`` file,
or you :ref:`provide a "logback.xml" <application-logback>` with your application
before it is deployed.


.. _logging-monitoring-system-services-logs:

CDAP System Services Logs
=========================
As CDAP system services run either on cluster edge nodes or in YARN containers, their
logging and :ref:`its configuration <logging-monitoring-system-services-logs-configuration>` 
depends on the service and where it is located.

Retrieving Log Messages from a System Service
---------------------------------------------
The log messages emitted by CDAP system services can be retrieved by:

- Using the :ref:`restful-api`: the :ref:`Logging HTTP RESTful API
  <http-restful-api-logging>` details :ref:`downloading the logs
  <http-restful-api-logging-downloading-system-logs>` emitted by a system service.
  
- Log messages of system services can be viewed in the :ref:`CDAP UI <cdap-ui>`. In the
  *Administration* page (accessible through the *CDAP* menu on the far-right) click either
  a *Logs* icon (such as the *HBase* log icon circled in red, in the *Component Overview*
  section):

  .. figure:: /_images/logging/logging-cdap-ui-administration.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image

      **CDAP UI:** HBase Component, with its *Log* icon circled in red

  You can click on a CDAP system service and a window will appear displaying a log icon
  (in this case, after clicking *App Fabric*), as shown here circled in red:

  .. figure:: /_images/logging/logging-cdap-ui-administration-app-fabric.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image

      **CDAP UI:** CDAP *App Fabric* popup window, with its *Log* icon circled in red

System Service Log File Locations
---------------------------------
The location of CDAP system service logs depends on the mode of CDAP (Standalone or
Distributed) and the Hadoop distribution:

- For **Standalone CDAP:** system logs are located in ``<CDAP-SDK-HOME>/logs``.

- For **Distributed CDAP:** system logs are located in ``/var/log/cdap`` (with the
  exception of Cloudera Manager-based clusters). With Cloudera Manager installations, system
  log files are located in directories under ``/var/run/cloudera-scm-agent/process``.

.. _logging-monitoring-system-services-logs-configuration:

Configuring System Service Logs
-------------------------------
- CDAP system services that run in YARN containers, such as the *Metrics Service*, are
  configured by the same ``logback-container.xml`` that :ref:`configures user application
  program logging <configuring-user-application-program-logging>`.

- CDAP system services that run on cluster edge nodes, such as CDAP Master or Router, are
  configured by the ``logback.xml`` Changes can be made to ``logback.xml``; afterwards,
  the service(s) affected will need to be restarted for the modified "logback" file to
  take effect.

  - For **Standalone CDAP:** the ``logback.xml`` file is located in ``/etc/cdap/conf``.
  - For **Distributed CDAP:** the file ``logback.xml`` file, located in 
    ``<cdap-sdk-home>/conf``, configures both "container" and CDAP system service logging.

Changing System Service Log Levels
----------------------------------
When running under Distributed CDAP, the log levels of system services can be changed at
runtime without either modifying the ``logback.xml`` or restarting CDAP.

The CDAP :ref:`Logging HTTP RESTful API
<http-restful-api-logging-changing-system-service-log-levels>` can be used to set the log
levels of a system service while it is running. Once changed, they can be reset back to
what they were originally by using the :ref:`reset endpoint
<http-restful-api-system-service-logging-resetting>`.

The REST endpoints can be applied to all system services listed at :ref:`Logging HTTP
RESTful API <http-restful-api-logging-changing-system-service-log-levels>`. However, since
``appfabric`` and ``dataset.service`` are running on the same node, changing log levels of the 
``appfabric`` service **will also** change the log levels of the ``dataset.service``.

**Note:** The :ref:`Logging HTTP RESTful API
<http-restful-api-logging-changing-system-service-log-levels>` for changing system service
log levels can only be used with system services that are running under Distributed CDAP.
For changing the log levels of system services under Standalone CDAP, you need to modify
the ``logback.xml`` file and restart CDAP.


.. _logging-monitoring-configuring-the-log-saver-service:

Configuring the Log Saver Service
=================================
The Log Saver Service is the CDAP service that reads log messages from Kafka, processes
them in *log pipelines*, persists them to HDFS, and sends metrics on logging to the
Metrics Service.

In addition to the default *CDAP Log Pipeline*, you can specify :ref:`custom log pipelines
<logging-monitoring-custom-log-pipelines>` that are run by the log saver service and
perform custom actions.

The :ref:`cdap-site.xml <appendix-cdap-site.xml>` file has properties that control the
writing of logs to Kafka, the log saver service, the CDAP log pipeline, and any
:ref:`custom log pipelines <logging-monitoring-custom-log-pipelines>` that have been defined.

.. _logging-monitoring-writing-logs-to-kafka:

Writing Logs to Kafka
---------------------
These :ref:`properties <appendix-cdap-default-logging>` control the writing of logs to
Kafka:

.. generated by code in conf.py for this manual
.. include:: /../target/_includes/logging-writing-logs-to-kafka.rst

**Notes:**

- If an external Kafka service is used (instead of the CDAP Kafka service), the
  number of partitions used for ``log.publish.num.partitions`` must match the number set
  in the external service for the topic being used to publish logs (``log.kafka.topic``).

- By default, ``log.publish.partition.key`` is set to ``program``, which means that all
  logs for the same program go to the same partition. Set this to ``application`` if you
  want all logs from an application to go to the same instance of the Log Saver Service.

Log Saver Service
-----------------
These :ref:`properties <appendix-cdap-default-logging>` control the Log Saver Service:

.. generated by code in conf.py for this manual
.. include:: /../target/_includes/logging-log-saver-service.rst

Log saver instances should be from a minimum of one to a maximum of ten. The maximum is
set by the number of Kafka partitions (``log.publish.num.partitions``), which by default is 10.

.. _logging-monitoring-log-pipeline-configuration:

Log Pipeline Configuration
--------------------------
Configuration properties for logging and :ref:`custom log pipelines
<logging-monitoring-configuring-custom-log-pipelines>` are shown in the documentation of the
:ref:`logging properties <appendix-cdap-default-logging>` section of the
:ref:`cdap-site.xml <appendix-cdap-site.xml>` file.

The **CDAP log pipeline** is configured by settings in the :ref:`cdap-site.xml
<appendix-cdap-site.xml>` file.

**Custom log pipelines** are configured by a combination of the settings in the 
:ref:`cdap-site.xml <appendix-cdap-site.xml>` file and a "logback" file used
to specify the custom pipeline. The XML file is placed in the
``log.process.pipeline.config.dir``, a local directory on the CDAP Master node that is scanned
for log processing pipeline configurations. Each pipeline is defined by a file in the
Logback XML format, with ``.xml`` as the file name extension.

These :ref:`properties <appendix-cdap-default-logging>` control the CDAP log pipeline:

.. generated by code in conf.py for this manual
.. include:: /../target/_includes/logging-pipeline-configuration-1.rst

.. _logging-monitoring-cdap-site-xml-configuration:

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
      
.. figure:: /_images/logging/logging-framework.png
    :figwidth: 100%
    :width: 800px
    :align: center

    **CDAP Logging Framework:** From YARN containers, through Kafka and the Log Saver
    Service, to HDFS

- Logs are collected from individual programs running in YARN containers.

- YARN writes the log messages emitted by containers to files inside the container.

- In addition, CDAP programs publish these messages to Kafka.

- CDAP System Services run (depending on the service) either on cluster edge nodes or in
  YARN containers. Where they run determines the file that configures that service's
  logging.

- The Log Saver Service (*log.saver*) is configured to read log messages for the
  ``logs.user-v2`` Kafka topic (set by the property ``log.kafka.topic``). The number of
  log saver instances can be scaled to process the Kafka partitions in parallel, if
  needed.
  
  Log saver, by default, runs only the CDAP Log Pipeline: it reads the messages from
  Kafka, groups them by program or application, buffers and sorts them in memory, and
  finally persists them to files in HDFS. Each of these files corresponds to one program
  or application, depending on how the grouping is configured. (This is set by the
  property :ref:`log.publish.partition.key <logging-monitoring-writing-logs-to-kafka>`,
  described :ref:`below <logging-monitoring-configuring-the-log-saver-service>`.)

  *Note:* These files are configured to rotate based on time and size; they can be
  changed using the properties ``log.pipeline.cdap.file.max.size.bytes`` and
  ``log.pipeline.cdap.file.max.lifetime.ms`` in the :ref:`cdap-site.xml
  <appendix-cdap-default-logging>` file as described in 
  :ref:`logging-monitoring-log-pipeline-configuration`.

- For security, the files written out to persistent storage in HDFS have permissions set
  such that they are accessible only by the ``cdap`` user.

- In addition, custom log pipelines can be configured by adding an XML file in a
  prescribed location. Each pipeline buffers log messages in memory and sorts them based
  on their timestamp. 
 
- In addition to persisting logs to files, the log saver also emits metrics about the
  number of log messages emitted by each program. These metrics can be retrieved by
  querying the :ref:`CDAP metrics system <http-restful-api-metrics>`.
  
  These tables list the metrics from the section :ref:`available-system-metrics` of the
  :ref:`http-restful-api-metrics`. See that section for further information.
  
  .. These tables are copied from cdap-docs/reference-manual/source/http-restful-api/metrics.rst
  .. and are not guarded.

  .. list-table::
     :widths: 50 50

     * - **Application Logging Metric**
       - **Description**
     * - ``system.app.log.{error, info, warn}``
       - Number of ``error``, ``info``, or ``warn`` log messages logged by an application or
         applications
     * - **System Services Logging Metric**
       - **Description**
     * - ``system.services.log.{error, info, warn}``
       - Number of ``error``, ``info``, or ``warn`` log messages logged by a system service
         or system services


.. _logging-monitoring-custom-log-pipelines:

Custom Log Pipelines
====================
For a custom log pipeline, create and configure a "logback" file,
configuring loggers, appenders, and properties based on your requirements, and place the
file at the path specified in the ``cdap-site.xml`` file by the property
``log.process.pipeline.config.dir`` of the ``cdap-site.xml`` file.

Each custom pipeline requires a unique name. Properties controlling the pipeline (the
``log.process.pipeline.*`` properties) are described :ref:`above
<logging-monitoring-cdap-site-xml-configuration>`.

For every XML file in the ``log.process.pipeline.config.dir`` directory, a separate log
pipeline is created. As they are separate Kafka consumers and processes, each pipeline is
isolated and independent of each other. The performance of one pipeline does not affect
the performance of another. Though CDAP has been tested with multiple log pipelines and
appenders, the fewer of each that are specified will provide better performance.

.. _logging-monitoring-configuring-custom-log-pipelines:

Configuring Custom Log Pipelines
--------------------------------
CDAP looks for "logback" files located in a directory as set by the property
``log.process.pipeline.config.dir`` in the :ref:`cdap-site.xml
<appendix-cdap-default-logging>` file. In the default configuration, this is:
  
- For **Standalone CDAP:** ``<cdap-sdk-home>/ext/logging/config``
- For **Distributed CDAP:** ``/opt/cdap/master/ext/logging/config``

.. _logging-monitoring-custom-logging-example:

Example "logback" File for a Custom log pipeline
----------------------------------------------------
.. highlight:: xml

Here is an example "logback" file, using two appenders (``STDOUT`` and
``rollingAppender``). This file must be located (as :ref:`noted above
<logging-monitoring-configuring-custom-log-pipelines>`) with a file extension of ``.xml``::

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

Custom Log Appender
-------------------
You can use any existing `logback <https://logback.qos.ch/manual/appenders.html>`__
appender. The ``RollingLocationLogAppender`` |---| an extension of the Logback
``FileAppender`` |---| lets you use HDFS locations in your log pipelines. 

If you need an appender beyond what is available through Logback or CDAP, you can write
and implement your own custom appender. See the `Logback documentation
<https://logback.qos.ch/manual/appenders.html>`__ for information on this.

.. highlight:: java

As the CDAP LogFramework uses Logback's Appender API, your custom appender needs to
implement the same Appender interface. Access to CDAP's system components (such as
datasets, metrics, ``LocationFactory``) are made available to the ``AppenderContext``, an
extension of Logback's ``LoggerContext``::

  public class CustomLogAppender extends FileAppender<ILoggingEvent> implements Flushable, Syncable {
    . . .
    private LocationManager locationManager;

    @Override
    public void start() {
      if (context instanceof AppenderContext) {
        AppenderContext context = (AppenderContext) this.context;
        locationManager = new LocationManager(context.getLocationFactory() . . .);
        . . .
      }
    }

    @Override
    public void doAppend(ILoggingEvent eventObject) throws LogbackException {
      try {
        . . .
        OutputStream locationOutputStream = locationManager.getLocationOutputStream . . .
        setOutputStream(locationOutputStream);
        writeOut(eventObject);
        . . .
      } catch
       . . .
    }
  }

Adding a dependency on the ``cdap-watchdog`` API will allow you to access the
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

