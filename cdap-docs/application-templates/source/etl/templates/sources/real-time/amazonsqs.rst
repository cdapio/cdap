.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

==============================
Sources: Real-time: Amazon SQS
==============================

.. rubric:: Description

Amazon SQS (Simple Queue Service) Real-time Source that emits a record with a field
'body' of type String.

.. rubric:: Use Case

This source is used when you want to read messages from Amazon SQS. For example,
a web beacon is pushing log records to SQS and you want to read these log events
in real-time. We can use this source to read these records and then store them
in a Cube.

.. rubric:: Properties

**region:** Region where the queue is located.

**accessKey:** Access Key of the AWS (Amazon Web Services) account to use.

**accessID:** Access ID of the AWS account to use.
  
**queueName:** Name of the queue.
  
**endpoint:** Endpoint of the SQS server to connect to. Omit this field to connect to AWS.

.. rubric:: Example

::

  {
    "name": "SQS",
    "properties": {
      "region": "us-west-1",
      "accessID": "accessID",
      "accessKey": "accessKey",
      "queueName": "queue_name"
    }
  }

This example reads in from a queue named 'queue_name' which is hosted on a server that's
located in the 'us-west-1' region.
