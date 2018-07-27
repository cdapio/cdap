.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2018 Cask Data, Inc.

:hide-toc: true

.. _cloud-runtimes-example:

====================
Example Walk-Through
====================

This example walks you through creating a profile using the Google Dataproc provisioner
and then running a pipeline on a Google Dataproc cluster using the new profile.

Setup
-----

The Google Dataproc provisioner requires that you perform a little bit of setup on your Google Cloud Platform (GCP) account.
You can perform all these steps in the GCP Console.

  - Enable Cloud Dataproc
  - Create a service account key that has permission to create Cloud Dataproc clusters and view Google Compute Engine resources
  - Ensure there is an ingress VPC firewall rule that allows TCP connections on port 443 (HTTPS)
  - Create a Google Cloud Storage bucket and upload a test file

After setting up your GCP account, use the :ref:`Secure Storage RESTful API <http-restful-api-secure-storage>`
to add the service account key as a key in the CDAP secure store.
For example, to add a secure key named `gcp-key` using curl::

  $ curl localhost:11015/v3/namespaces/default/securekeys/gcp-key -X PUT -d '{
    "properties": {},
    "description": "gcp service account key",
    "data": "{  \"type\": \"service_account\", \"private_key\": \"...\", ... }"
  }'

The value of the data field is the content of your GCP service account key in string form.

Create a Profile
----------------

To create a profile, navigate to the Administration page using the menu at the top right of the CDAP UI.
From this page, you can view the list of system profiles.

.. figure:: /_images/cloud-runtimes/create-system-profile.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Click the button to create a new profile.

.. figure:: /_images/cloud-runtimes/provisioners.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Select the Google Cloud Dataproc provisioner.

.. figure:: /_images/cloud-runtimes/provisioner-configure.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Fill in all the configuration fields. For the service account key, specify the name of the key that
you added during setup.

Create a Test Pipeline
----------------------

Click the green plus button and create a new batch pipeline. This simple test pipeline reads a file in Cloud Storage
and writes to an output directory in Cloud Storage.
Begin by adding a File source and a File sink from the left panel. Connect the source to the sink.

.. figure:: /_images/cloud-runtimes/simple-pipeline-overview.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Configure the source to read the test file from the Cloud Storage bucket that you created during setup.

.. figure:: /_images/cloud-runtimes/simple-pipeline-input.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Configure the sink to write to a directory that does not yet exist.

.. figure:: /_images/cloud-runtimes/simple-pipeline-output.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

To deploy the pipeline, click the `Deploy` button near the top right of the screen, which opens the pipeline overview page.
In this page, configure the pipeline to use the profile that you created earlier.
This will bring you to the pipeline overview page. Configure the pipeline to use the profile that was created earlier.

.. figure:: /_images/cloud-runtimes/pipeline-profile-manual.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Note that you can also override any profile setting that was not locked during profile creation.

.. figure:: /_images/cloud-runtimes/pipeline-profile-override.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Run the pipeline. On the pipeline overview page, you can monitor the progress of the pipeline.
`Pending` means Cloud Dataproc is provisioning the cluster for the run. After the cluster is
running, the pipeline status transitions to `Starting` as CDAP prepares to run the pipeline
on the newly created cluster. The pipeline status transitions to `Running` once the pipeline
begins execution on the cluster. Once the pipeline completes, the status transitions to `Succeeded`.
At this point, the Cloud Dataproc cluster will be deleted.

After the pipeline runs, navigate to the Administration page and click your profile to view its details.

.. figure:: /_images/cloud-runtimes/profile-overview.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

The Administration page shows you usage metrics for the profile and its assigned pipelines.

You have now successfully created and used a profile to execute a pipeline in a cloud environment.

