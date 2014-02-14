:orphan:

.. server-config-reference:

.. index::
   single: Server Configuration
====================
Server Configuration
====================

.. include:: /guide/admin/admin-links.rst

Configuring the server:
---------------------------------

Loom server uses Zookeeper for task coordination and a database for persistent data. The server will work
without any additional configuration options by generating an in memory Zookeeper and an embedded Derby DB; however, we 
strongly recommend that administers supply their own instances of persistent storage or database for performance and 
maintainability. Below we indicate how you supply or configure your own database (in this case mySQL server) for storage 
and the associated JDBC connectors in a xml configuration file.

Zookeeper
^^^^^^^^^
The zookeeper quorum, a collection of nodes running instances of Zookeeper, is specified as a comma delimited list of ``<host>:<port>`` (e.g. ``server1:2181,server2:2181,server3:2181``).

Database
^^^^^^^^
Loom uses JDBC for database access. To provide your own database, and for Loom to access it, you must specify a driver, a connection string, 
a user, and a password, as shown in the following example. Depending on the network configuration, the host that the database server will 
bind to must be defined in the ``loom.host`` xml element option:
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
      <description>Specifies the loom host to bind to</description>
    </property>
    <property>
      <name>loom.jdbc.driver</name>
      <value>com.mysql.jdbc.Driver</value>
      <description>specifies db driver</description>
    </property>
    <property>
      <name>loom.jdbc.connection.string</name>
      <value>jdbc:mysql://127.0.0.1:3306/loom?useLegacyDatetimeCode=false</value>
      <description>specifies how to connect to mysql</description>
    </property>
    <property>
      <name>loom.db.user</name>
      <value>loom</value>
      <description>mysql user</description>
    </property>
    <property>
      <name>loom.db.password</name>
      <value>loomers</value>
      <description>mysql user password</description>
    </property>
  </configuration>

A full list of available configuration settings and their default values are given below:

.. list-table::

  * - Config setting
    - Default
    - Description
  * - zookeeper.quorum
    - A local value determined by an in-memory Zookeeper
    - Zookeeper quorum for the server.
  * - zookeeper.namespace
    - "/loom"
    - Namespace to use in Zookeeper
  * - loom.port
    - 55054
    - Port for the server
  * - loom.host
    - "localhost"
    - IP address for the database server to bind to
  * - loom.jdbc.driver
    - org.apache.derby.jdbc.EmbeddedDriver
    - JDBC driver to use for database operations
  * - loom.jdbc.connection.string
    - "jdbc:derby:/var/loom/data/db/loom;create=true"
    - JDBC connection string to user for database operations
  * - loom.db.user
    - "loom"
    - Database user
  * - loom.db.password
    - null
    - Database password
  * - loom.solver.num.threads
    - 20
    - Number of threads used for solving cluster layout
  * - loom.local.data.dir
    - "/var/loom/data"
    - Local data directory that default in-memory Zookeeper and embedded Derby will use.
  * - loom.task.timeout.seconds
    - 3600
    - Seconds before the server will timeout a provisioner's task and marks it as failed
  * - loom.cluster.cleanup.seconds
    - 180
    - Interval in seconds between each check for tasks to time out
  * - loom.netty.exec.num.threads
    - 50
    - Number of execution threads for the server
  * - loom.netty.worker.num.threads
    - 20
    - Number of worker threads for the server
  * - loom.node.max.log.length
    - 2048
    - Max log size in bytes for capturing stdout and stderr for actions performed on cluster nodes. Logs longer than set limit will be trimmed from the head of the file. 
  * - loom.node.max.num.actions
    - 200
    - Max number of actions saved for a node. Oldest action will be removed when actions exceeding this limit are performed on a node.
