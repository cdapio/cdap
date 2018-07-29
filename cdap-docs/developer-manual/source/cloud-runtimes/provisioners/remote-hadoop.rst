.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2018 Cask Data, Inc.

.. _cloud-runtimes-provisioners-remote-hadoop:

=============
Remote Hadoop
=============

The Remote Hadoop provisioner is used to run jobs on a pre-existing Hadoop cluster.
This provisioner does not create or delete a cluster; it simply connects to one.
We assume that the provided cluster is running compatible versions of all the software that CDAP requires.
The cluster must not use Kerberos authentication and must be accessible via SSH and HTTPS (ports 22 and 443).

The configuration for the Remote Hadoop provisioner is the same information required to SSH to an edge node of the cluster.

Host
----
The IP address or hostname of the edge node that CDAP will SSH to when starting
the pipeline run. The node must be able to submit YARN jobs.

User
----
The user to SSH as.

SSH Key
-------
The SSH private key to use when connecting to the cluster. Since your SSH key is sensitive,
we recommended that you provide the key through the CDAP :ref:`Secure Storage <http-restful-api-secure-storage>` API
by adding a secure key with the RESTful API and clicking the shield icon in the UI to select a secure key.

