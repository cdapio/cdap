.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016-2017 Cask Data, Inc.

.. _admin-impersonation:

=============
Impersonation
=============

Impersonation allows users to run programs and access datasets, streams, and other
resources as pre-configured users. Currently, CDAP supports configuring impersonation
at a namespace level, which means that every namespace has a single user that all
programs in that namespace run as, and that resources are accessed as.

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
instead.

The configured Kerberos principal must have been granted permissions for the operations
that will occur in that namespace. For instance, if
a :ref:`custom HBase namespace <namespaces-custom-mapping>` is configured, the configured
principal must have privileges to create tables within that HBase namespace. If no
custom HBase namespace is specified, the configured principal must have privileges to
create namespaces.

Because of this, it is simplest to specify a custom mapping for ``root.directory`` and
``hbase.namespace`` when using impersonation so that the privileges granted to the
configured principal can be kept to a minimum.


Limitations
===========
The configured HDFS delegation token timeout must be longer than the configured stream
partition duration (``stream.partition.duration``), which has a default value of
one hour (3600000). It must also be larger than the log saver's maximum file
lifetime (``log.saver.max.file.lifetime.ms``), which has a value of six hours (21600000).


Known Issues
============
- :cask-issue:`CDAP-8140` - Explore is not supported when impersonation is enabled with Hive 0.13.
