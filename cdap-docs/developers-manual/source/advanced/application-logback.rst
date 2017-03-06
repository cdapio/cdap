.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015-2017 Cask Data, Inc.

.. _application-logback:

===================
Application Logback
===================

.. highlight:: console

Logging Framework

CDAP collects logs and metrics for all of its internal services and and user applications.

To do so, CDAP uses a logging framework that consists of *appenders* and *logging pipelines*.

**An appender** is a Java class, responsible for writing log events to persistent storage and
maintaining metadata about that storage, and implementing the <> and <> interfaces.
CDAP includes a RollingLocationLogAppender that is used for this.

**A logging pipeline** is a single-threaded process that reads log events from Kafka, and invokes
the appender methods defined in its configuration.


In the default configuration, CDAP uses a single 



YARN containers launched by a CDAP application use a default container logback file
|---| ``logback-container.xml`` |---| packaged with CDAP and installed in 
the CDAP :ref:`configuration directory <admin-manual-cdap-components>`. This logback does
log rotation once every day at midnight and deletes logs older than 14 days. Depending on
the use case, the default configuration may be sufficient. As long as the container is
running, these policies will apply.

**Note:** In the case of the CDAP Standalone, the logback used is ``logback.xml``, located
in the ``<cdap-sdk-home>/conf`` directory.

However, you can specify a custom ``logback.xml`` for a CDAP application by packaging
it with the application in the application's ``src/main/resources`` directory.
The packaged ``logback.xml`` is then used for each container launched by the application.

To write a custom logback, refer to the `Logback manual <http://logback.qos.ch/manual>`__
for information.

**Note:** When a custom ``logback.xml`` is specified for an application, the custom
``logback.xml`` will be used in place of the ``logback-container.xml``. A custom
``logback.xml`` needs to be configured for log rotation (``rollingPolicy``) and log
clean-up (``maxHistory``) to ensure that long-running containers don't fill up the disk.
