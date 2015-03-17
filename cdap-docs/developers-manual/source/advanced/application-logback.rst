.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _application-logback:

====================
Application Logbacks
====================

.. highlight:: xml

YARN containers launched by a CDAP application use the default container logback file
present in :ref:Configuration <install-configuration> directory - ``logback-container.xml``,
packaged with CDAP. This logback does log rotation once every day at midnight and
deletes logs older than 14 days. Depending on the use case, the default configuration may be sufficient.
However, it is also possible to specify a custom ``logback.xml`` for a CDAP application by packaging
it with the application in the application's ``src/main/resources`` directory.
The packaged ``logback.xml`` is then used for each container launched by the application.

To write a custom logback, you may refer to `Logback <http://logback.qos.ch/>`__.

**Note:** When a custom ``logback.xml`` is specified for an application, the custom ``logback.xml``
will be used in place of ``logback-container.xml``. The custom ``logback.xml`` needs to be configured
for log rotation and log clean-up to ensure that long-running containers don't fill up the disk.
