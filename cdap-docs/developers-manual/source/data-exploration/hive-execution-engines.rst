.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _hive-execution-engines:

======================
Hive Execution Engines
======================

Overview
--------
As of CDAP 3.4.0, CDAP Explore has added support for two additional execution engines: 
`Apache Spark <http://spark.apache.org/>`__ and 
`Apache Tez <http://tez.apache.org/>`__.

.. _hive-execution-engines-hive-on-spark:

Hive on Spark (Experimental)
----------------------------
Using this feature, you can configure CDAP Explore to use `Apache Spark <http://spark.apache.org/>`__ as the
execution engine. To use this feature, you should `configure Hive to use Spark as the execution engine
<https://cwiki.apache.org/confluence/display/Hive/Hive+on+Spark%3A+Getting+Started#HiveonSpark:GettingStarted-ConfiguringHive>`__
in the ``hive-site.xml`` file that is used by CDAP. In particular, these properties need to be set:

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Parameter
     - Value
   * - ``hive.execution.engine``
     - ``spark``
   * - ``spark.eventLog.enabled``
     - ``true``
   * - ``spark.eventLog.dir``
     - Path to the Spark event log directory (must exist)
   * - ``spark.executor.memory``
     - ``512m``
   * - ``spark.serializer``
     - ``org.apache.spark.serializer.KryoSerializer``
   * - ``spark.driver.memory``
     - ``1g``

This feature is currently experimental in CDAP due to these limitations:

- It requires Spark to be installed on all cluster nodes.
- Currently, CDAP Explore launches a new Spark job for every query. Starting a new Spark job for every query may bring
  a significant overhead.
- Users cannot dynamically select the execution engine, or adjust memory for containers created for the query.

.. _hive-execution-engines-hive-on-tez:

Hive on Tez
-----------
Using this feature, you can configure CDAP Explore to use `Apache Tez <http://tez.apache.org/>`__ 
as the execution engine. To use this feature, you should `configure Hive to use Tez as the execution engine 
<https://cwiki.apache.org/confluence/display/Hive/Hive+on+Tez>`__.

In addition, you should set these environment variables in ``cdap-env.sh``:

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Environment Variable
     - Value
   * - ``TEZ_HOME``
     - Path to home directory of the Apache Tez installation
   * - ``TEZ_CONF_DIR``
     - Path to the directory where ``tez-site.xml`` is located
