.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _admin-impersonation:

=============
Impersonation
=============

Impersonation allows users to run programs and access datasets, streams, and other
resources as pre-configured users. Currently CDAP supports configuring impersonation
at a namespace level, which means that every namespace will have a single user that
programs in that namespace will run as, and that resources will be accessed as.

Requirements
============
To utilize this feature, Kerberos must be enabled on the cluster and configured in
:ref:`cdap-site.xml <appendix-cdap-site.xml>`, via the parameter ``kerberos.auth.enabled``.
To configure a namespace to have impersonation, specify the ``principal`` and
``keytabURI`` in the :ref:`namespace configuration <http-restful-api-namespace-configs>`.

If these are not specified, the principal and keytab of the CDAP Master user will be used.

The configured Kerberos principal must have been granted permissions for the operations
that will occur in that namespace. For instance, if
a :ref:`custom HBase namespace <namespaces-custom-mapping>` is configured, the configured
principal must have privileges to create tables within that HBase namespace. If no
custom HBase namespace is specified, the configured principal must have privilege to
create namespaces.
Because of this, it is simplest to specify a custom mapping for ``root.directory`` and
``hbase.namespace`` when using impersonation, so that the privileges granted to the
configured can be kept minimal.


Limitations
===========
The configured HDFS delegation token timeout must be longer than the configured stream
partition duration (``stream.partition.duration``), which has a default value of
one hour (3600000). It must also be larger than log saver's maximum
life time (``log.saver.max.file.lifetime.ms``), which has a default value of six hours.

Known Issues
============
- :cask-issue:`CDAP-6587` - Explore is not supported when Impersonation is enabled.
