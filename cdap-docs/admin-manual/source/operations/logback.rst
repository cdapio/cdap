.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

====================================
Master Service Logging Configuration
====================================

:term:`Master Services <master services>` use ``logback-container.xml``, packaged with the CDAP distribution,
for logging configuration. This logback does log rotation once a day at midnight and expires logs older than
14 days. Changes can be made to ``logback-container.xml``, and the ``cdap-master`` process will need to be restarted
for the modified logback to take effect.