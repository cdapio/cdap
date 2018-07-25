.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2018 Cask Data, Inc.

.. _cloud-runtimes-provisioners-gcp-dataproc:

===============
Google Dataproc
===============

Cloud Dataproc is a Google Cloud Platform (GCP) service that manages Hadoop clusters in the cloud and
can be used to create large clusters quickly. The Google Dataproc provisioner simply calls the Cloud Dataproc APIs to create
and delete clusters in your GCP account. The provisioner exposes several configuration settings that control what type of cluster is created.

Account Information
-------------------

Project ID
^^^^^^^^^^
A GCP project ID must be provided. This will be the project that the Cloud Dataproc cluster is created in.
The project must have the Cloud Dataproc APIs enabled.

Service Account Key
^^^^^^^^^^^^^^^^^^^
The service account key provided to the provisioner must have rights to access the Cloud Dataproc APIs and the Google Compute Engine APIs.
Since your account key is sensitive, we recommended that you provide your account key through the CDAP
:ref:`Secure Storage <http-restful-api-secure-storage>` by adding a secure key with the RESTful API and clicking the shield icon
in the UI to select a secure key.

Size Settings
-------------
Size settings control how big of a cluster to create.

Master Nodes
^^^^^^^^^^^^
The number of master nodes to have in your cluster. Master nodes contain the YARN Resource Manager,
HDFS NameNode, and will be the node that CDAP will connect to when executing jobs. Must be set to either 1 or 3.

Master Cores
^^^^^^^^^^^^
The number of virtual cores to allocate to each master node.

Master Memory
^^^^^^^^^^^^^
The amount of memory in gigabytes to allocate to each master node.

Worker Nodes
^^^^^^^^^^^^
The number of worker nodes to have in your cluster. Worker nodes contain the YARN NodeManager and HDFS DataNode.
They perform the actual work during job execution.

Worker Cores
^^^^^^^^^^^^
The number of virtual cores to allocate for each worker node.

Worker Memory
^^^^^^^^^^^^^
The amount of memory in gigabytes to allocate to each worker node.

General Settings
----------------

Network
^^^^^^^
This is the VPC network in your GCP project that will be used when creating a Cloud Dataproc cluster.

Region
^^^^^^
A region is a specific geographical location where you can host resources, like the compute nodes
for your Cloud Dataproc cluster.

Zone
^^^^
A zone is an isolated location within a region.

Polling Settings
----------------
Polling settings control how often cluster status should be polled when creating and deleting clusters.
You may want to change these settings if you have a log of pipelines scheduled to run at the same time
using the same GCP account.

Create Poll Delay
^^^^^^^^^^^^^^^^^
The number of seconds to wait after creating a cluster to begin polling to see if the cluster has been created.

Create Poll Jitter
^^^^^^^^^^^^^^^^^^
Maximum amount of random jitter in seconds to add to the create poll delay. This is used to prevent a lot of
simultaneous API calls against your GCP account when you have a lot of pipelines that are scheduled to run at
the exact same time.

Delete Poll Delay
^^^^^^^^^^^^^^^^^
The number of seconds to wait after deleting a cluster to begin polling to see if the cluster has been deleted.

Poll Interval
^^^^^^^^^^^^^
The number of seconds to wait in between polls for cluster status.

