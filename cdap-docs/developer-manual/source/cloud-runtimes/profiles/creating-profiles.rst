.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2018 Cask Data, Inc.

.. _cloud-runtimes-profiles-creating-profiles:

=================
Creating Profiles
=================

Profiles tell CDAP which provisioner to use when creating a cluster and specify the cluster configuration.
They also specify the provisioner configuration that should be used when creating a cluster.

Native Profile
--------------
The native profile is a special profile that runs programs natively in the same environment that CDAP runs in.
Before Cloud Runtimes were introduced, this was the only way to run programs.
With the CDAP Sandbox, this means programs run in the same JVM as CDAP.
With CDAP Distributed, this means programs run in the same YARN cluster that CDAP is running in.

Scope
-----
Each profile has a scope: `system` or `user`. You can use system profiles in any namespace.
User profiles exist within a namespace, and only programs in that namespace can use user profiles.

To create a system profile, navigate to the CDAP administration page.
This page includes a list of all system profiles and the button to create a system profile.

To create a user profile, navigate to the CDAP administration page and then navigate to the namespace that you want to create the profile in.
From there, you can create a profile that exists only within that namespace.

Creation
--------

To create a profile, first select the provisioner that the profile uses to create and configure the cloud runtime.

.. figure:: /_images/cloud-runtimes/provisioners.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Once you select a provisioner, you need to provide the configuration that the provisioner requires.

.. figure:: /_images/cloud-runtimes/provisioner-configure.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Note that the fields marked with the shields contain sensitive information, such as secret keys.
We recommend that you provide sensitive information through the CDAP
:ref:`Secure Storage <http-restful-api-secure-storage>` API, and you can do this by adding a secure key with the RESTful API.
Then, click the shield icon in the UI to select a secure key.

In general, provisioner configuration can be overridden at runtime. To lock certain settings so that they cannot be overridden,
click the lock symbol to make the value immutable.

