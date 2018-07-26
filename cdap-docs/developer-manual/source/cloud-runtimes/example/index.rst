.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2018 Cask Data, Inc.

:hide-toc: true

.. _cloud-runtimes-example:

====================
Example Walk-Through
====================

This example walks you through creating a profile using the Google Dataproc provisioner,
then running a pipeline on a Google Dataproc cluster using the new profile.

Setup
-----

The Google Dataproc provisioner requires that you perform a little bit of setup on your Google Cloud Platform (GCP) account.
All of these steps can be performed in the GCP Console.

  - Enable Cloud Dataproc
  - Create a service account key that has permission to create Cloud Dataproc clusters and view Google Compute Engine resources
  - Ensure there is an ingress VPC firewall rule that allows tcp connections on port 443 (https)
  - Create a Google Cloud Storage (GCS) bucket and upload a test file

After you have set up your GCP account, add the service account key as a key in the CDAP secure store.
This must be done using the :ref:`Secure Storage RESTful API <http-restful-api-secure-storage>`.
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

Click on the button to create a new profile. 

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

Fill in all configuration fields. For the service account key, specify the name of the secure key that
was added in the setup part of this walk-through.

Create a Test Pipeline
----------------------

Click on the green plus button and create a new batch pipeline. Our test pipeline will be a very simple
one that simply reads a file on GCS and writes to an output directory on GCS. Begin by adding a File source
and a File sink from the left panel. Connect the source to the sink.

.. figure:: /_images/cloud-runtimes/simple-pipeline-overview.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Configure the source to read a file from the GCS bucket that you set up earlier.

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

Deploy the pipeline by clicking on the `Deploy` button near the top right of the screen.
This will bring you to the pipeline overview page. Configure the pipeline to use the
profile that was created earlier.

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

Run the pipeline. The pipeline should transition to the `Pending` status while the Cloud Dataproc
cluster is being provisioned for the run. You can view the Cloud Dataproc section of the GCP Console
to track progress of the cluster. After the cluster has been provisioned, you will see the pipeline
status transition to `Starting`, then `Running`, then `Succeeded`. This, of course, assumes that your
GCP account and your pipeline are properly configured.

After the pipeline has run, navigate back to the Administration page and click on your profile to view
details about the profile.

.. figure:: /_images/cloud-runtimes/profile-overview.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

From this page, you can see usage metrics for the profile, as well as what pipeline have been assigned to it.
You have now successfully created a profile and used it to execute a pipeline in a cloud environment.

