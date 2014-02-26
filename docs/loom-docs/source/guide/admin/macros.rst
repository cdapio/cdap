:orphan:

.. macros-reference:

.. index::
   single: Macros
======
Macros
======

.. include:: /guide/admin/admin-links.rst

Often at template creation time, certain parameters cannot be set because their values are unknown or their values can
be only ascertained during runtime. For instance, at the time of configuring a cluster template, it is impossible to know the 
hostnames or IP addresses of other nodes within a cluster. For this reason, Loom supports several macros that allow administrators
to programmatically set these values ahead of time as place holders, which later can be substituted with actual values. 

Basic Macros
============

Macros are supported in the config section of a template and are denoted by surrounding ``%``
symbols or strings. Basic macros allow you to specify a comma delimited list of hostnames or IP addresses of nodes that provide a
specific service. The basic macros available are:
::
  %host.service.<service name>%
  %ip.service.<service name>%
  %num.service.<service name>%

Basic macros return a comma separated list of the specified entity for all nodes that contain the specified service.
For example, '%host.service.datanode%' will be replaced with a comma separated list of all nodes in the cluster that
have the datanode service.

Basic Macro Instances
=====================
Basic macros refer to an entity for a group of nodes containing a specific service. Sometimes, it is 
necessary to refer to an entity for a single node that contains a specific service.  This can be done with basic
macro instances:
::
  %host.self.service.<service name>%
  %ip.self.service.<service name>%
  %instance.self.service.<service name>%
  %host.service.<service name>[instance number]%
  %ip.service.<service name>[instance number]%

Instead of returning a comma separated list of hosts or ips, an instance macro returns a single value corresponding
to the entity of a single node that contains the specified service. For example, '%host.service.zookeeper[0]%' gets
replaced with the hostname of the first node that has the zookeeper service. Instance macros can also change depending on the node 
that gets the config with the macros. For example, '%instance.self.service.zookeeper%' will get expanded to '1'
on the first node that has zookeeper, and expanded to '2' on the second node that has zookeeper and so on.   


Macro Functions
===============

Two macro functions are also supported in Loom: join and map.
::
  %join(map(host.service.<service name>,'<format string>'),'<delimiter>')%
  %join(host.service.<service name>,'<delimiter>')%

Join takes 2 arguments. The first argument is a basic macro or a map macro, and the second argument is a delimiter used to join them.
Map takes 2 arguments, the first argument is a basic macro and the second argument is a format string, where the format
gets applied to each entity in the basic macro. When the format is applied, the '$' character in the format string is
replaced by whatever value the entity resolves to. 

Examples
========

Macros are best described with examples, and they are very common during installation, automation and configuration tasks.

Suppose you are configuring a HDFS + HBase cluster that uses three Zookeeper
nodes. A user creates a cluster from this template, and Loom decides to place the namenode and
hbase-master services on a node with hostname ``hadoop-dev-1001.local`` and IP address ``123.456.0.1``. It then decides to place
the three Zookeeper services on their own nodes with hostnames ``hadoop-dev-[1002-1004].local`` with IP addresses ``123.456.0.[2-4]``.
One configuration setting each datanode needs is called ``fs.defaultFS``, and it needs to be set to a value that contains
the hostname of the namenode. This can be achieved with the following basic macro:
::
  "fs.defaultFS":"hdfs://%host.service.hadoop-hdfs-namenode%"

Loom will see the macro ``%host.service.hadoop-hdfs-namenode%`` and replace it with a comma separated list of all the hosts
that contain the ``hadoop-hdfs-namenode`` service. Since the namenode service is only installed on one node, it will become:
::
  "fs.defaultFS":"hdfs://hadoop-dev-1001.local"

Similarly, if we had used the ``%ip.service.hadoop-hdfs-namenode%`` macro instead, it would resolve to:
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

Similarly, if we wanted IP addresses, we could have replaced ``host.service.zookeeper-server`` with ``ip.service.zookeeper-server``.


Now, let's look at the configuration for Zookeeper. Each Zookeeper configuration file needs the list of servers
and their hostnames. To be more specific, part of each config file needs to contain:
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
comma-separated list of all hosts with the ``zookeeper-server`` service, it's replaced by the hostname of the *i*'th
node with ``zookeeper-server``, where *i* is specified in the square brackets.

Each Zookeeper service also needs to know its ID. Taking the above example, ``hadoop-dev-1002.local`` needs to know that
its ID is 1 (matching the ``server.1`` setting), ``hadoop-dev-1003.local`` needs to know that its ID is 2 (again matching
the ``server.2`` setting), and so on. To achieve this, we can use the self macro:
::
  "myId":"%instance.self.service.zookeeper-server%"

This variable will be replaced by the instance number of that service, beginning at 1. To be exact,
``hadoop-dev-1002.local`` will receive ``"myId":"1"``, ``hadoop-dev-1003.local`` will receive ``"myId":"2"``, and
``hadoop-dev-1004.local`` will receive ``"myId:3"``. Similarly, if a node needs its own hostname or IP address, you can use
``%host.self.service.[service name]%`` and ``%ip.self.service.[service name]%``, respectively. Finally, say if you need a
configuration setting that needs to resolve to the number of zookeeper-servers running in your cluster. This
can be achieved with the ``%num.service.zookeeper-server%`` macro.
