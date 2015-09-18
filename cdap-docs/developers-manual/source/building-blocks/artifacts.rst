.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _artifacts:

=========
Artifacts
=========

.. highlight:: java

An *Artifact* is a JAR file that contains Java classes and resources required to create and run an *Application*.

*Artifacts* are identified by a name, version, and scope.
The artifact name must consist of only alphanumeric, '-', and '_' characters. For example,
'my-application' is a valid artifact name, but 'my:application' is not.

The artifact version is of the format '[major].[minor].[fix](-|.)[suffix]'. Minor, fix, and suffix
portions of the version are optional, though it is suggested that you have them conform to
standard versioning schemes. The major, minor, and fix portions of the version must be numeric.
The suffix can be any of the acceptable characters. For example, '3.2.0-SNAPSHOT' is a valid artifact version,
with a major version of 3, minor version of 2, fix version of 0, and suffix of SNAPSHOT. 

The artifact scope is either 'user' or 'system'. An artifact in the 'user' scope was added by users
through the CLI or RESTful API. A 'user' artifact belongs in a namespace and cannot be accessed in
another namespace. A 'system' artifact is an artifact that is available across all namespaces. It
is added by placing the artifact in a special directory on either the CDAP master node(s) or the
CDAP Standalone. 

Once added to CDAP, an *Artifact* cannot be modified unless it is a snapshot artifact.
An *Artifact* is a snapshot artifact if the version suffix begins with SNAPSHOT. For example,
'1.2.3.SNAPSHOT-test' is a snapshot version because it has a suffix of 'SNAPSHOT-test', which
begins with SNAPSHOT. '1.2.3' is not a snapshot version because there is no suffix. '1.2.3-hadoop2'
is also not a snapshot version because the suffix does not begin with SNAPSHOT.

.. rubric:: Adding an Artifact

.. rubric:: Plugin Artifacts

.. rubric:: Deleting an Artifact

.. rubric:: System Artifacts

