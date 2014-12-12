.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Troubleshooting CDAP
============================================

Here are some selected examples of potential problems and possible resolutions.


.. rubric:: Application Won't Start

Check HDFS write permissions. It should show an obvious exception in the YARN logs.
 

.. rubric:: CDAP services on distributed CDAP don’t start up due to ``java.lang.ClassNotFoundException``

If the CDAP services on a distributed CDAP environment wouldn't start up, you will see errors
in the logs. You will find in the logs for ``cdap-master`` under ``/var/log/cdap/master*.log``
errors such as these::

 "Exception in thread "main" java.lang.NoClassDefFoundError:
   co.cask.cdap.data.runtime.main.MasterServiceMain
     at gnu.java.lang.MainThread.run(libgcj.so.10)"

Things to check as possible solutions:

1. Check if the JDK being used is :ref:`supported by CDAP<install-java-runtime>`::

    java -version

#. Check if the CDAP user is using a correct version of the JDK::

    sudo su - <cdap-user> 
    java -version
   
#. Run this command to see if all the CDAP classpaths are included::

    /opt/cdap/master/bin/svc-master classpath | tr ':' '\n'
   
   Expect to see (where *<version>* is one of ``0.94``, ``0.96``, or ``0.98``)::

    /etc/cdap/conf/
    /opt/cdap/hbase-compat-<version>/lib/*
    /opt/cdap/master/conf/
    /opt/cdap/master/lib/*

   If the classpath is incorrect, review the :ref:`installation instructions <install>` and correct.
   

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

By default, the Log Saver process is limited to 1GB of memory and the buffer keeps eight buckets of events
in-memory. Each event bucket contains logs generated for one second. When it is expected that logs exceeding
these settings will be produced—for example, greater than 1GB of logs generated in eight seconds—increase
the memory allocated to the Log Saver or increase the number of Log Saver instances. If the cluster has
limited memory or containers available, you can choose instead to decrease the number of in-memory event buckets.
However, decreasing the number of in-memory buckets may lead to out-of-order log events.

In the ``cdap-site.xml``, you can:

- Increase the memory by adjusting ``log.saver.run.memory.megs``;
- Increase the number of Log Saver instances using ``log.saver.num.instances``; and
- Adjust the number of in-memory log buckets ``log.saver.event.max.inmemory.buckets``.

See the ``log.saver`` parameter section of the :ref:`Appendix cdap-site.xml
<appendix-cdap-site.xml>` for a list of these configuration parameters and their
values that can be adjusted.
