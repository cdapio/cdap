.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _apache-hadoop-kms:

=========================================
Apache Hadoop Key Management Server (KMS)
=========================================

CDAP integrates with `Apache Hadoop Key Management Server (KMS) 
<https://hadoop.apache.org/docs/stable/hadoop-kms/index.html>`__
as the backend for :ref:`Secure Storage <admin-secure-storage>`. To use this
secure storage implementation, set ``security.store.provider`` to ``kms`` in ``cdap-site.xml``.

Prerequisites
=============
Since KMS is only available in Apache Hadoop as of version 2.6.0, this secure storage implementation
can only be used on clusters with Apache Hadoop 2.6.0 or later installed.

The KMS path should be available as the ``hadoop.security.key.provider.path`` property of either ``core-site.xml``
or ``hdfs-site.xml`` on all cluster hosts. Refer to
`KMS Client Configuration <https://hadoop.apache.org/docs/stable/hadoop-kms/index.html>`__ for the expected format
of the value of this property.

Additionally, the ``/etc/hadoop/kms-acls.xml`` file on the KMS host should be updated to include users 
with appropriate permissions. 

- If :ref:`impersonation <admin-impersonation>` is enabled and KMS-backed secure storage is used from
  programs, the impersonated user should be provided appropriate permissions in the ``/etc/hadoop/kms-acls.xml``.

- If it is used through the :ref:`Secure Storage HTTP RESTful API <http-restful-api-secure-storage>`, the CDAP
  logged-in user should be provided appropriate permissions in the ``/etc/hadoop/kms-acls.xml``.

On a cluster managed with Cloudera Manager, these permissions can be set in the *Key Management Server
Advanced Configuration Snippet (Safety Valve) for kms-acls.xml* setting on the *Configuration* page for KMS.
