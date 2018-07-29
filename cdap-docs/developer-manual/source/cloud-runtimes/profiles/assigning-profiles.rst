.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2018 Cask Data, Inc.

.. _cloud-runtimes-profiles-assigning-profiles:

==================
Assigning Profiles
==================

You can assign profiles to batch pipelines in the following ways:

  - Assign a default profile for all of CDAP, as well as a default profile for a specific namespace.
  - Assign a profile to a batch pipeline to use for runs that are started manually.
  - Assign a profile to a pipeline schedule.

The profile that is used at runtime is determined by walking the CDAP hierarchy from most specific to most general.

If a profile is set on the schedule that triggered a run, that profile is used.
If no profile is set, the default profile for the namespace is used.
If no default is set on the namespace, the default for CDAP is used.
If no CDAP default is set, the native profile is used.

Similarly, if a pipeline is manually run and there is a profile assigned to that pipeline, that profile is used.
If no profile is set, the default profile for the namespace is used.
If no default is set on the namespace, the default for CDAP is used.
If no CDAP default is set, the native profile is used.

Note: If you have deployed your own custom applications and are using workflows,
you can also use profiles to run your workflow in a cloud environment.
Batch pipelines are just workflows with special UI support that makes profiles are easy to use.

Assigning Default Profiles
--------------------------
You can assign default profiles to a namespace and to the CDAP instance through the UI.
Navigate to the Administration page and click the star next to the profile.

.. figure:: /_images/cloud-runtimes/namespace-default-profile.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Alternatively, you can use the :ref:`Preferences RESTful API <http-restful-api-preferences>` to set default profiles.

  - To set the default profile for all of CDAP, set a preference on the CDAP instance with key `system.profile.name` and value `system:<profile-name>`.
  - To set the default profile for a namespace, set a preference on the desired namespace with key `system.profile.name` and value `<scope>:<profile-name>`.

Assigning a Profile for Manual Runs
-----------------------------------
You can assign a profile to use for manual pipeline runs.
Navigate to the pipeline detail page and click the configure button > Advanced Options > Compute config.
Next, select a profile and save. From that point on, the profile is used any time the pipeline is run manually.

.. figure:: /_images/cloud-runtimes/pipeline-profile-manual.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Alternatively, you can use the :ref:`Preferences RESTful API <http-restful-api-preferences>` to set the profile for manual runs
by setting preference on the DataPipelineWorkflow entity with key `system.profile.name` and value `<scope>:<profile-name>`.

Assigning a Profile to a Schedule
---------------------------------
Any time you create a schedule for a pipeline, you can assign a profile to it.
Whenever the schedule triggers a pipeline run, it will use that profile for the run.
This is true for time schedules and schedules that other pipelines trigger.

.. figure:: /_images/cloud-runtimes/schedule-profile.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Alternatively, you can use the :ref:`Schedules RESTful API <http-restful-api-lifecycle-schedule-add>` to assign a profile to a schedule.

Overriding Profile Configuration
--------------------------------
When a profile is created, each configuration settings can be made immutable by locking it.
All other configuration settings can be overridden at runtime. You can edit configuration settings
in the same page where you assign the profile.

.. figure:: /_images/cloud-runtimes/pipeline-profile-override.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

You can use runtime arguments and schedule properties to modify the size of the cluster or other important settings.

To override the profile used, set a runtime argument with key `system.profile.name` and value `<scope>:<profile-name>`.

To override a profile property, set a runtime argument with key `system.profile.properties.<propety-name>`
and value equal to the desired value for that property. For example, to override the numWorkerssetting to a value of 10,
set a preference or runtime argument with key `system.profile.properties.numWorkers` and value 10.

