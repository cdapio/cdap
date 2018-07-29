.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2018 Cask Data, Inc.

:hide-toc: true

.. _cloud-runtimes-concepts:

========
Concepts
========

Cloud runtimes are configured through profiles and created by provisioners.

Provisioners
------------

A provisioner is responsible for creating and tearing down the cloud cluster where
the batch pipeline will be executed. Different provisioners are capable of creating
different types of clusters on various clouds. Currently, there is a Google Dataproc
provisioner, an Amazon Elastic MapReduce (EMR) provisioner, and a Remote Hadoop provisioner.

Each provisioner exposes a set of configuration settings that control the type of cluster that will be created for a run.
For example, the Google Dataproc and Amazon EMR provisioners have configuration settings for the size of the cluster.
Provisioners also have settings for the credentials required to talk to their respective clouds and provision the required compute nodes.
Profiles provide these configuration settings.

Profiles
--------

A profile is a CDAP entity that specifies a provisioner name and a set of configuration settings for that provisioner.
CDAP administrators create profiles. Profiles can be assigned to batch pipelines.
When a profile is assigned to a pipeline, the provisioner specified in the profile will be used to create a cluster
where the pipeline will run -- instead of running the pipeline in the native CDAP cluster.

For example, an administrator might decide to create small, medium, and large profiles.
Each profile is configured with the Google Cloud Platform credentials required to
create and delete Dataproc clusters in the company’s cloud account.
The small profile is configured to create a 5-node cluster.
The medium profile is configured to create a 20-node cluster.
The large profile is configured to create a 50-node cluster.
The administrator assigns the small profile to pipelines that are scheduled to run every hour on small amounts of data
while the large profile is assigned to pipelines that are scheduled to run every day on a large amount of data.

