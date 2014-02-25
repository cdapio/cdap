:orphan:

.. server-config-reference:

.. index::
   single: Server Configuration
====================
Server Configuration
====================

.. include:: /guide/admin/admin-links.rst

Configuring the server
----------------------

Loom server uses Zookeeper for task coordination and a database for persistent data. The server will work out of the box
without any configuration options with an in memory Zookeeper and an embedded Derby DB; however, we 
strongly recommend that administrators supply their own Zookeeper quorum and database outside of Loom for performance and 
maintainability. Below we indicate how you can supply your own database (in this case MySQL server) for storage, 
and the associated JDBC connector in the server configuration file.

Zookeeper
^^^^^^^^^
The zookeeper quorum, a collection of nodes running instances of Zookeeper, is specified as a comma delimited list of ``<host>:<port>`` (e.g. ``server1:2181,server2:2181,server3:2181``).

Database
^^^^^^^^
Loom uses JDBC for database access. To provide your own database, and for Loom to access it, you must specify a driver, a connection string, 
a user, and a password, as shown in the following example.  We also recommend specifying a validation query to be used with jdbc connection 
pooling.  The query will change based on which database you are using.  
::
  <?xml version="1.0"?>
  <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
  <configuration>
    <property>
      <name>zookeeper.quorum</name>
      <value>loom.dev.continuuity.net:2181</value>
      <description>Specifies the zookeeper host:port</description>
    </property>
    <property>
      <name>loom.host</name>
      <value>162.242.148.244</value>
      <description>Specifies the hostname/IP address for the server to bind to</description>
    </property>
    <property>
      <name>loom.jdbc.driver</name>
      <value>com.mysql.jdbc.Driver</value>
      <description>Specifies DB driver</description>
    </property>
    <property>
      <name>loom.jdbc.connection.string</name>
      <value>jdbc:mysql://127.0.0.1:3306/loom?useLegacyDatetimeCode=false</value>
      <description>Specifies DB connection string</description>
    </property>
    <property>
      <name>loom.db.user</name>
      <value>loom</value>
      <description>DB user</description>
    </property>
    <property>
      <name>loom.db.password</name>
      <value>loomers</value>
      <description>DB user password</description>
    </property>
    <property>
      <name>loom.jdbc.validation.query</name>
      <value>SELECT 1</value>
      <description>query used with connection pools to validate a jdbc connection taken from a pool</description>
    </property>
  </configuration>

Configuration
^^^^^^^^^^^^^

A full list of available configuration settings and their default values are given below:

.. list-table::
   :header-rows: 1

   * - Config setting
     - Default
     - Description
   * - zookeeper.quorum
     - A local value determined by an in-memory Zookeeper.
     - Zookeeper quorum for the server.
   * - zookeeper.namespace
     - "/loom"
     - Namespace to use in Zookeeper
   * - zookeeper.session.timeout.millis
     - 40000
     - Zookeeper session timeout value in milliseconds.
   * - loom.port
     - 55054
     - Port for the server.
   * - loom.host
     - "localhost"
     - Hostname/IP address for the server to bind to.
   * - loom.jdbc.driver
     - org.apache.derby.jdbc.EmbeddedDriver
     - JDBC driver to use for database operations.
   * - loom.jdbc.connection.string
     - "jdbc:derby:/var/loom/data/db/loom;create=true"
     - JDBC connection string to user for database operations.
   * - loom.jdbc.validation.query
     - "VALUES 1" when using default for loom.jdbc.driver (Derby), null otherwise.
     - Validation query used by JDBC connection pool to validate new DB connections.  mysql, postgres, and microsoft sql server can use "select 1".  oracle can use "select 1 from dual".
   * - loom.jdbc.max.active.connections
     - 100
     - Maximum active JDBC connections.
   * - loom.db.user
     - "loom"
     - Database user.
   * - loom.db.password
     - null
     - Database password.
   * - loom.solver.num.threads
     - 20
     - Number of threads used for solving cluster layout.
   * - loom.local.data.dir
     - "/var/loom/data"
     - Local data directory that default in-memory Zookeeper and embedded Derby will use.
   * - loom.task.timeout.seconds
     - 1800
     - Number of seconds the server will wait before timing out a provisioner task and marking it as failed.
   * - loom.cluster.cleanup.seconds
     - 180
     - Interval, in seconds, between server housekeeping runs. Housekeeping includes timing out tasks, expiring clusters, etc.
   * - loom.netty.exec.num.threads
     - 50
     - Number of execution threads for the server.
   * - loom.netty.worker.num.threads
     - 20
     - Number of worker threads for the server.
   * - loom.node.max.log.length
     - 2048
     - Maximum log size in bytes for capturing stdout and stderr for actions performed on cluster nodes. Logs longer than set limit will be trimmed from the head of the file.
   * - loom.node.max.num.actions
     - 200
     - Maximum number of actions saved for a node. Oldest action will be removed when actions exceeding this limit are performed on a node.
   * - loom.max.cluster.size
     - 10000
     - Maximum number of nodes that a given cluster can be created with.
   * - loom.max.action.retries
     - 3
     - Maximum number of times a task gets retried when it fails.
   * - scheduler.run.interval.seconds
     - 1
     - Interval, in seconds, various runs are scheduled on the server.
