.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2018 Cask Data, Inc.

.. _cloud-runtimes-provisioners-aws-emr:

========================
Amazon Elastic MapReduce
========================

Elastic MapReduce is an Amazon Web Services product, which provides a managed cluster platform for running
big data processing and analysis on frameworks such as Apache Hadoop and Apache Spark.
The Amazon EMR provisioner simply calls the EMR APIs in order to create and delete
clusters in your AWS account.
The provisioner exposes several configuration settings that control what type of cluster is created.

Account Information
-------------------

Access Key
^^^^^^^^^^
An AWS Access Key ID identifies an AWS access key, which can be used to make secure REST or HTTP query
protocol requests to AWS service APIs.

Secret Key
^^^^^^^^^^
An AWS Secret Key can be used to make secure REST or HTTP query protocol requests to AWS service APIs.
Since your secret key is sensitive, we recommended that you provide the key through the
CDAP :ref:`Secure Storage <http-restful-api-secure-storage>` API by adding a secure key with the RESTful API
and clicking the shield icon in the UI to select a secure key.

General Settings
----------------

Region
^^^^^^
When you launch an Amazon EMR cluster, you must specify a region. You might choose a region to reduce
latency, minimize costs, or address regulatory requirements. For more information, refer to
https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-region.html.

EC2 Subnet
^^^^^^^^^^
A subnet defines a subset of a VPC (virtual private cloud) dedicated to your AWS account. For more
information, refer to https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-vpc-launching-job-flows.html
and https://docs.aws.amazon.com/AmazonVPC/latest/UserGuide/VPC_Subnets.html#vpc-subnet-basics.

Additional Master Security Group
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
An additional EMR managed security group that will be applied to the master instance. It must have
an inbound rule that allows ssh and https (ports 22 and 443). For more information, refer to
https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-man-sec-groups.html.

Logs URI
^^^^^^^^
Copy the cluster's log files automatically to S3. For more information, refer to
https://docs.aws.amazon.com/console/elasticmapreduce/logging.

Instances
---------

Number of instances
^^^^^^^^^^^^^^^^^^^
Total number of instances in the cluster. One of them will be a master instance.

Master instance type
^^^^^^^^^^^^^^^^^^^^
The instance type for the master instance.
Instance types comprise varying combinations of CPU, memory, storage, and networking capacity and give you the flexibility to choose the appropriate mix of resources for your applications.
For more information, refer to https://aws.amazon.com/ec2/instance-types/ and https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-supported-instance-types.html.

Worker instance type
^^^^^^^^^^^^^^^^^^^^
The instance type for the worker instances.
Instance types comprise varying combinations of CPU, memory, storage, and networking capacity and give you the flexibility to choose the appropriate mix of resources for your applications.
For more information, refer to https://aws.amazon.com/ec2/instance-types/ and https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-supported-instance-types.html.
