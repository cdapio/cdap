:orphan:

.. macros-reference:
.. include:: /toplevel-links.rst

======
Macros
======

.. include:: /guide/admin/admin-links.rst

When setting the configuration in a cluster template, an administrator may often need to refer to other nodes in the cluster.
However, it is impossible to know hostnames and IP addresses prior to cluster creation. For this purpose, Loom supports several
macros that can be used to programmatically represent these values ahead of time.

Basic Macros
============

Macros are supported in the config section of a template and are denoted by surrounding ``%``
symbols. Basic macros allow you to specify a comma delimited list of hostnames or IP addresses of nodes that provide a
specific service. The basic macros available are:
::
  %host.service.[service name]%
  %ip.service.[service name]%
  %num.service.[service name]%
  %host.self.service.[service name]%
  %ip.self.service.[service name]%
  %instance.self.service.[service name]%

Macros are formatted in a period-delimitted manner. The first value represents the type of value that will be used to replace the macro.
``host`` refers to a hostname of a node with specified service, ``ip`` refers to an IP address of a node with the service, ``num`` refers to
a number of nodes with the service, and ``instance`` refers to an instance number of a service.

Macro Functions
===============

Two Macro functions are also supported in Loom. These are explicitly:
::
  %join(map(host.service.[service name],'[format string]'),'[delimiter]')%
  %join(host.service.[service name],'[format string]','[delimiter'])%

These functions are better described with examples. Suppose you are configuring a HDFS + HBase cluster that uses three Zookeeper
nodes. A user creates a cluster from this template, and Loom decides to place the namenode and
hbase-master services on a node with hostname ``hadoop-dev-1001.local`` and IP address ``123.456.0.1``. It then decides to place
the three Zookeeper services on their own nodes with hostnames ``hadoop-dev-[1002-1004].local`` with IP addresses ``123.456.0.[2-4]``.
One configuration setting each datanode needs is called ``fs.defaultFS``, and it needs to be set to a value that contains
the hostname of the namenode. This can be achieved like such:
::
  "fs.defaultFS":"hdfs://%host.service.hadoop-hdfs-namenode%"

Loom will see the macro ``%host.service.hadoop-hdfs-namenode%`` and replace it with a comma separated list of all the hosts
that contain the ``hadoop-hdfs-namenode`` service. Since the namenode service is only installed on one node, it will become:
::
  "fs.defaultFS":"hdfs://hadoop-dev-1001.local"

Similarly, if we had used the ``%ip.service.hadoop-hdfs-namenode%`` macro instead, it would have become:
::
  "fs.defaultFS":"hdfs://123.456.0.1"

When configuring the HBase master, we need to specify a Zookeeper quorum and namespace.
In this case, we want to end up with:
::
  hbase.zookeeper.quorum":"hadoop-dev-1002.local:2181,
    hadoop-dev-1003.local:2181,hadoop-dev-1004.local:2181/namespace

This can be achieved with the ``join`` and ``map`` macro:
::
  hbase.zookeeper.quorum":"%join(map(host.service.zookeeper-server,'$:2181'),',')%/namespace

To step through this command, the ``map`` function takes two arguments. The first is an entity to use, and the second is a
string that specifies the format. The format string will then be mapped to each entity for each node that contains the service. In this case,
the entity is ``host.service.zookeeper-server`` and the format is ``$:2181``, which means the hostname of each node
that contains the zookeeper-server will be mapped to ``hostname:2181``. This also means ``hadoop-dev-1002.local`` gets
mapped to ``hadoop-dev-1002.local:2181``, ``hadoop-dev-1003.local`` gets mapped to ``hadoop-dev-1003.local:2181``, and
``hadoop-dev-1004.local`` gets mapped to ``hadoop-dev-1004.local:2181``. Finally, these mappings are sent to the ``join``
function, which takes each element and joins them together with the delimiter specified, which in this case is a comma.
The config setting eventually becomes:
::
  hbase.zookeeper.quorum":"hadoop-dev-1002.local:2181,
    hadoop-dev-1003.local:2181,hadoop-dev-1004.local:2181/namespace

Similarly, if we wanted IP addresses, we could have replaced ``host.service.zookeeper-server`` with ``ip.service.zookeeper-server``.


Zookeeper Configuration Macros
==============================

Now we want to look at the configuration for Zookeeper. Each Zookeeper configuration file needs the list of servers
and the hostnames. To be more concrete, part of each config file needs to contain:
::
  server.1=hadoop-dev-1002.local:2888:3888
  server.2=hadoop-dev-1003.local:2888:3888
  server.3=hadoop-dev-1004.local:2888:3888

To do this, we can use the following macros in the template configuration:
::
  "server.1":"%host.service.zookeeper-server[0]%:2888:3888"
  "server.2":"%host.service.zookeeper-server[1]%:2888:3888"
  "server.3":"%host.service.zookeeper-server[2]%:2888:3888"

These macros work just like the ``%host.service.zookeeper-server%`` macro, but rather than being replaced by a
comma-separated list of all hosts with the ``zookeeper-server`` service, it gets replaced by the hostname of the *i*'th
node with ``zookeeper-server``, where *i* is specified in the square brackets.

Each Zookeeper service also needs to know it's ID. Take the above example, ``hadoop-dev-1002.local`` needs to know that
it's ID is 1 (matching the ``server.1`` setting), ``hadoop-dev-1003.local`` needs to know that it's ID is 2 (again matching
the ``server.2`` setting), and so on. To achieve this, we can use the self macro:
::
  "myId":"%instance.self.service.zookeeper-server%"

This variable will be replaced by the instance number of that service, beginning at 1. To be exact,
``hadoop-dev-1002.local`` will receive ``"myId":"1", ``hadoop-dev-1003.local`` will receive ``"myId":"2"``, and
``hadoop-dev-1004.local`` will receive ``"myId:3"``. Similarly, if a node needs its own hostname or IP address, you can use
``%host.self.service.[service name]%`` and ``%ip.self.service.[service name]%``, respectively. Finally, say you need a
configuration setting that needs to resolve the number of zookeeper-servers running in your cluster. This
can be achieved with the ``%num.service.zookeeper-server%`` macro.