.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016-2017 Cask Data, Inc.

.. _admin-impersonation:

=============
Impersonation
=============

Impersonation allows users to run programs and access datasets, streams, and other
resources as pre-configured users (a *principal*). Currently, CDAP supports configuring
impersonation at a namespace and at an application level, with application level
configuration having a higher precedence than namespace level.

Namespace-level impersonation means that every namespace has a single principal that all
programs in that namespace run as, and that resources are accessed as.

Application-level impersonation which means that every application has a single principal
that all programs in that application run as, and that resources are accessed as. Any
streams or datasets created by the application would be owned by that user.

Streams and datasets created outside of an application can be created with a principal;
otherwise, they would be owned by the principal defined for the namespace in which they
are created.


Requirements
============
To utilize this feature, `Kerberos <http://kerberos.org>`__ must be enabled on the cluster and
configured in :ref:`cdap-site.xml <appendix-cdap-site.xml>`, using the parameter ``kerberos.auth.enabled``.

To configure a namespace to have impersonation, specify the Kerberos ``principal`` and
``keytabURI`` in the :ref:`namespace configuration <http-restful-api-namespace-configs>`.
The keytab file (the "keytab") must be readable by the CDAP user and can be on either the local file system
of the CDAP Master or on HDFS. If the keytab is on HDFS, prefix the path with ``hdfs://``.
If CDAP Master is :ref:`HA-enabled <admin-manual-install-deployment-architectures-ha>`, 
and the local file system is used, the keytab must be on all local file systems used with 
the CDAP Master instances.

If these are not specified, the principal and keytab of the CDAP Master user will be used
instead. These are defined by the properties ``cdap.master.kerberos.principal`` and
``cdap.master.kerberos.keytab`` respectively in the :ref:`cdap-site.xml file
<appendix-cdap-default-security>`.

The configured Kerberos principal must have been granted permissions for the operations
that will occur in that namespace. For instance, if a :ref:`custom HBase namespace
<namespaces-custom-mapping>` is configured, the configured principal must have privileges
to create tables within that HBase namespace. If no custom HBase namespace is specified,
the configured principal must have privileges to create namespaces.

Because of this, it is simplest to specify a custom mapping for ``root.directory`` and
``hbase.namespace`` when using impersonation so that the privileges granted to the
configured principal can be kept to a minimum.

HDFS Permissions
----------------
In the case of impersonation, *every user who can be impersonated* will need access to
their corresponding HDFS ``/user/<username>`` directory. The commands for this are
described in the installation section for each distribution (:ref:`Cloudera Manager
<cloudera-hdfs-permissions>`, :ref:`Ambari <ambari-hdfs-permissions>`, 
:ref:`MapR <mapr-hdfs-permissions>`, and :ref:`packages <packages-hdfs-permissions>`).

Application-level Impersonation
-------------------------------
To use application-level impersonation in CDAP |---| where applications, datasets, and streams have
their own owner and the operations performed in CDAP impersonate their respective
owners |---| CDAP needs to have access to the owner principal and their associated keytabs.

.. highlight:: xml

For user's keytab access, CDAP uses these conventions:

- All keytabs must be present on the local filesystem of nodes on which the CDAP Master is running. 
- These keytabs must be present under a path which can be in one of these formats
  and the ``cdap`` system user should have read access to all of the keytabs::

    /<dir-1>/<dir-2>/${name}.keytab
    /<dir-1>/<dir-2>/${name}/${name}.keytab

- The above path is provided to CDAP as a configuration parameter in the ``cdap-site.xml``
  file, such as::

    <property>
        <name>security.keytab.path</name>
        <value>/etc/security/keytabs/${name}.keytab</value>
    </property>

  where ``${name}`` will be replaced by CDAP by the short user name of the Kerberos
  principal CDAP is impersonating.
  
  **Note:** You will need to restart CDAP for this configuration change to take effect.

Owner principal of an entity is provided either when an entity is created using the CDAP
CLI or the RESTful APIs or when an application creates them.

Hive Configuration
------------------
In order for Hive to work with impersonation, one of the following approaches can be used:

- Hive Proxy Users; or
- Hive SQL-based Authorization

**Hive Proxy Users**

To configure Hive to be able to impersonate other users, set in ``hive-site.xml`` the property::

  <property>
      <name>hive.server2.enable.doAs</name>
      <value>true</value>
  </property>

Note that the CDAP Explore service ignores this setting and needs to be able to
impersonate users who can create and access entities in CDAP. This can by done by adding
properties in your ``core-site.xml``. The first property allows Hive to impersonate users
belonging to ``group1`` and ``group2`` and the second property allows Hive to impersonate
on all hosts::

  <property>
      <name>hadoop.proxyuser.hive.groups</name>
      <value>group1,group2</value>
  </property>
 
  <property>
      <name>hadoop.proxyuser.hive.hosts</name>
      <value>*</value>
  </property>

See `Cloudera documentation
<http://www.cloudera.com/documentation/enterprise/latest/topics/cdh_sg_hive_metastore_security.html>`__
for additional details.

**Hive SQL-based Authorization**

An alternative to the above is to use SQL-based authorization. Add these properties to
your ``hive-site.xml``::

  <property>
      <name>hive.server2.enable.doAs</name>
      <value>false</value>
  </property>
  <property>
      <name>hive.security.authorization.manager</name>
      <value>org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory</value>
  </property>
  <property>
      <name>hive.security.authorization.enabled</name>
      <value>true</value>
  </property>
  <property>
      <name>hive.security.authenticator.manager</name>
      <value>org.apache.hadoop.hive.ql.security.ProxyUserAuthenticator</value>
  </property>

Note your hive-site.xml should also be configured to support modifying properties at
runtime. Specifically, you will need this configuration in your ``hive-site.xml``::

  <property>
      <name>hive.security.authorization.sqlstd.confwhitelist.append</name>
      <value>explore.*|mapreduce.job.queuename|mapreduce.job.complete.cancel.delegation.tokens|spark.hadoop.mapreduce.job.complete.cancel.delegation.tokens|mapreduce.job.credentials.binary|hive.exec.submit.local.task.via.child|hive.exec.submitviachild|hive.lock.*</value>
  </property>

After adding these properties to your ``hive-site.xml`` file, restart Hive.

CDAP Authorization
------------------
Impersonation works with CDAP Authorization, and if it is enabled, it will be enforced.
For details, see the sections on enabling on :ref:`enabling authorization in CDAP and
managing privileges <admin-authorization>`.


Limitations
===========
The configured HDFS delegation token timeout must be longer than the configured stream
partition duration (``stream.partition.duration``), which has a default value of
one hour (3600000). It must also be larger than the log saver's maximum file
lifetime (``log.saver.max.file.lifetime.ms``), which has a value of six hours (21600000).


Known Issues
============
- :cask-issue:`CDAP-8140` - Explore is not supported when impersonation is enabled with Hive 0.13.
