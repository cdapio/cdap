:orphan:

.. macros-reference:
.. include:: /toplevel-links.rst

==============================
Macro and Server Configuration
==============================

.. include:: /guide/admin/admin-links.rst

Configuring the server:
---------------------------------

Loom server uses Zookeeper for task coordination and a database to store persistent data.  The server will work
without any configuration options and will spin up an in memory Zookeeper and embedded Derby DB, but it is strongly
suggested that the user providers their own instances for performance and maintainability.  The zookeeper quorum is
specified as a comma delimited list of host and port (ex: server1:2181,server2:2181,server3:2181).  Loom uses JDBC,
so if you want to use your own database, you will need to specify a driver, a connection string, a user, and a
password as shown in the following example.  Depending on the network, you may also need to specify the host that
the server should bind to in the 'loom.host' option.

example:
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

  * - config setting
    - default
    - description
  * - zookeeper.quorum
    - some local value determined by in-memory zookeeper
    - zookeeper quorum for the server to use
  * - zookeeper.namespace
    - "/loom"
    - namespace to use in zookeeper
  * - loom.port
    - 55054
    - port for the server to use
  * - loom.host
    - "localhost"
    - address the server should bind to
  * - loom.jdbc.driver
    - org.apache.derby.jdbc.EmbeddedDriver
    - jdbc driver to use for database operations
  * - loom.jdbc.connection.string
    - "jdbc:derby:/var/loom/data/db/loom;create=true"
    - jdbc connection string to user for database operations
  * - loom.db.user
    - "loom"
    - database user
  * - loom.db.password
    - null
    - database password
  * - loom.solver.num.threads
    - 20
    - number of threads used for cluster layout solving
  * - loom.local.data.dir
    - "/var/loom/data"
    - local data directory that default in-memory zookeeper and embedded derby will use.
  * - loom.task.timeout.seconds
    - 3600
    - seconds before the server will timeout a provisioner task and mark it as failed
  * - loom.cluster.cleanup.seconds
    - 180
    - interval in seconds between each check for tasks to time out
  * - loom.netty.exec.num.threads
    - 50
    - number of execution threads for the server
  * - loom.netty.worker.num.threads
    - 20
    - number of worker threads for the server


Macros:
-----------

When setting the configuration in a cluster template, you will sometimes need to refer to other nodes in the cluster.
However, it is impossible to know hostnames and ips prior to cluster creation.  In this case, Loom supports several
macros that can be used.  Macros are only valid in the config section of a template.  They begin and end with the '%'
symbol.  Basic macros allow you to specify a comma delimited list of all hostnames or ips of nodes that have a
specific service on it.  Basic macros are:
::
  %host.service.[service name]%
  %ip.service.[service name]%
  %num.service.[service name]%
  %host.self.service.[service name]%
  %ip.self.service.[service name]%
  %instance.self.service.[service name]%

The first entity in the period delimited macro is the type of value that will be used to replace the macro.
'host' refers to a hostname of a node with the service, 'ip' to an ip address of a node with the service, 'num' to
a number of nodes with the service, and 'instance' refers to an instance number of a service.

There are also macro functions, which are:
::
  %join(map(host.service.[service name],'[format string]'),'[delimiter]')%
  %join(host.service.[service name],'[format string]','[delimiter'])%

This is most easily described with examples.  Suppose you are configuring a hdfs + hbase cluster that uses 3 zookeeper
nodes.  A user comes in and creates a cluster from your template, and Loom decides to place the namenode and
hbase-master services on a node with hostname ``'hadoop-dev-1001.local' and ip '123.456.0.1'``.  It decides to place
the 3 zookeeper services on their own nodes with hostnames ``'hadoop-dev-[1002-1004].local' with ips '123.456.0.[2-4]'``.
One configuration setting each datanode needs is called 'fs.defaultFS', and it needs to be set to a value that contains
the hostname of the namenode.  This can be achieved like:
::
  "fs.defaultFS":"hdfs://%host.service.hadoop-hdfs-namenode%"

Loom will see the macro %host.service.hadoop-hdfs-namenode% and replace it with a comma separated list of all the hosts
that contain the hadoop-hdfs-namenode service. Since the namenode service only goes on one node, it will become:
::
  "fs.defaultFS":"hdfs://hadoop-dev-1001.local"

Similarly, if we had used the ``%ip.service.hadoop-hdfs-namenode%`` macro instead, it would have become:
::
  "fs.defaultFS":"hdfs://123.456.0.1"

When configuring the hbase master, we need to specify a zookeeper quorum and namespace.
In this case, we want to end up with:
::
  "hbase.zookeeper.quorum":"hadoop-dev-1002.local:2181,
    hadoop-dev-1003.local:2181,hadoop-dev-1004.local:2181/namespace"

This can be achieved with the join and map macro:
::
  "hbase.zookeeper.quorum":"%join(map(host.service.zookeeper-server,'$:2181'),',')%/namespace"

To step through it, the map function takes 2 arguments.  The first is some entity to use, and the second is a format
string.  The format string will then be mapped to each entity for each node that contains the service.  In this case,
the entity is ``host.service.zookeeper-server`` and the format is ``'$:2181'``, which means the hostname of each node
that contains the zookeeper-server will be mapped to ``'hostname:2181'``.  This means ``hadoop-dev-1002.local`` gets
mapped to ``hadoop-dev-1002.local:2181``, ``hadoop-dev-1003.local`` gets mapped to ``hadoop-dev-1003.local:2181``, and
``hadoop-dev-1004``.local gets mapped to ``hadoop-dev-1004.local:2181``.  Finally, all of these are sent to the join
function, which takes each element and joins them together with the delimiter specified, which in this case is a comma
','.  This is how the config setting eventually becomes:
::
  "hbase.zookeeper.quorum":"hadoop-dev-1002.local:2181,
    hadoop-dev-1003.local:2181,hadoop-dev-1004.local:2181/namespace"

Similar, if we wanted ips, we could have replaced host.service.zookeeper-server with ip.service.zookeeper-server.

Now we want to look at the configuration for zookeeper.  Each zookeeper configuration file needs the list of servers
and the hostnames.  To be more concrete, part of each config file needs to contain:
::
  server.1=hadoop-dev-1002.local:2888:3888
  server.2=hadoop-dev-1003.local:2888:3888
  server.3=hadoop-dev-1004.local:2888:3888

To do this, we can use the following macros in the template configuration:
::
  "server.1":"%host.service.zookeeper-server[0]%:2888:3888"
  "server.2":"%host.service.zookeeper-server[1]%:2888:3888"
  "server.3":"%host.service.zookeeper-server[2]%:2888:3888"

These macros work just like the ``'%host.service.zookeeper-server%'`` macro, but instead of getting replaced by a
comma-separated list of all hosts with the zookeeper-server service, it gets replaced by the hostname of the *i*'th
node with zookeeper-server, where i is specified in the square brackets.

Each zookeeper service also needs to know it's id.  Take the above example, hadoop-dev-1002.local needs to know that
it's id is 1 (matching the server.1 setting), hadoop-dev-1003.local needs to know that it's id is 2 (again matching
the server.2 setting), and so on.  To do this, we can use the self macro:
::
  "myId":"%instance.self.service.zookeeper-server%"

This will be replaced by the instance number of that service, beginning at 1.  This means that
``hadoop-dev-1002.local`` will receive ``"myId":"1", hadoop-dev-1003.local will receive "myId":"2", and
hadoop-dev-1004.local will receive "myId:3"``.  Similarly, if a node needs its own hostname or ip, you can use
``'%host.self.service.[service name]%'`` and ``'%ip.self.service.[service name]%'``.  Finally, say you need a
configuration setting that needs to resolve to the number of zookeeper-servers you have in your cluster.  This
can be done with the ``'%num.service.zookeeper-server%'`` macro.