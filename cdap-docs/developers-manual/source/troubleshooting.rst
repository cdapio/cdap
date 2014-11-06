.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Troubleshooting CDAP
============================================

Here are some selected examples of potential problems and possible resolutions.


.. rubric:: Application Won't Start

Check HDFS write permissions. It should show an obvious exception in the YARN logs.
 

.. rubric:: No Metrics/logs

Make sure the *Kafka* server is running, and make sure local the logs directory is created and accessible.
On the initial startup, the number of available seed brokers must be greater than or equal to the
*Kafka* default replication factor.

In a two-box setup with a replication factor of two, if one box fails to startup,
metrics will not show up though the application will still run::

  [2013-10-10 20:48:46,160] ERROR [KafkaApi-1511941310]
        Error while retrieving topic metadata (kafka.server.KafkaApis)
        kafka.admin.AdministrationException:
               replication factor: 2 larger than available brokers: 1


.. rubric:: Only the First Flowlet Showing Activity

Check that YARN has the capacity to start any of the remaining containers.


.. rubric:: YARN Application Shows ACCEPTED For Some Time But Then Fails

It's possible that YARN can't extract the .JARs to the ``/tmp``,
either due to a lack of disk space or permissions.


.. rubric:: Log Saver Process Throws an Out-of-Memory Error, CDAP Console Shows Service Not OK

The CDAP Log Saver uses an internal buffer that may overflow and result in Out-of-Memory
Errors when applications create excessive amounts of logs. One symptom of this is that the CDAP
Console *Services Explorer* shows the ``log.saver`` Service as not OK, in addition to seeing error
messages in the logs.

By default, the buffer keeps 8 seconds of logs in memory and the Log Saver process is limited to 1GB of
memory. When it's expected that logs exceeding these settings will be produced, increase the memory
allocated to the Log Saver or increase the number of Log Saver instances. If the cluster has limited
memory or containers available, you can choose instead to decrease the duration of logs buffered in
memory. However, decreasing the buffer duration may lead to out-of-order log events.

In the ``cdap-site.xml``, you can:

- Increase the memory by adjusting ``log.saver.run.memory.megs``;
- Increase the number of Log Saver instances using ``log.saver.num.instances``; and
- Adjust the duration of logs with ``log.saver.event.processing.delay.ms``.

Note that it is recommended that ``log.saver.event.processing.delay.ms`` always be kept greater than
``log.saver.event.bucket.interval.ms`` by at least a few hundred (300-500) milliseconds.

See the ``log.saver`` parameter section of the :ref:`Appendix cdap-site.xml
<admin:appendix-cdap-site.xml>` for a list of these configuration parameters and their
values that can be adjusted.

