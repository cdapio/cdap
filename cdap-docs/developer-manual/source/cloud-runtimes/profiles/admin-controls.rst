.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2018 Cask Data, Inc.

.. _cloud-runtimes-profiles-admin-controls:

==============
Admin Controls
==============

There are several administrative controls in place to monitor and control profile usage.
To access the controls, navigate to the Administration page in CDAP and click a profile.

.. figure:: /_images/cloud-runtimes/profile-overview.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

The Administration page shows you information about which pipelines the profile is assigned to and profile usage.
For example, you can see how many runs ended in completion and failure when used with the profile or see an
estimate of the node hours spent using the profile.

In addition to viewing general usage metrics, you can disable a profile.
Note that any program that starts a run using a disabled profile will fail.
Additionally, you cannot assign disabled profiles to any entity.
This is useful if an administrator discovers that a profile is misbehaving or wants to lock down a profile that is resulting in excessive spending.

After meeting the following conditions, you can also delete a profile:

  - The profile is disabled.
  - The profile is not assigned to any entities. This means the profile cannot be the CDAP default profile or the default profile for any namespace.
  - The profile is not assigned to any pipeline schedules or assigned to a pipeline for manual runs.
  - The profile is not in use by any active program runs.

