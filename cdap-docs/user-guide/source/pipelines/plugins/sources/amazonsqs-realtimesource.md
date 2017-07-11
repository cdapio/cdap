# Amazon SQS


Description
-----------
Amazon SQS (Simple Queue Service) real-time source that emits a record with a field
'body' of type String.


Use Case
--------
This source is used when you want to read messages from Amazon SQS. For example,
a web beacon is pushing log records to SQS and you want to read these log events
in real-time. We can use this source to read these records and then store them
into a Cube.


Properties
----------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**region:** Region where the queue is located.

**accessKey:** Access Key of the AWS (Amazon Web Services) account to use.

**accessID:** Access ID of the AWS account to use.
  
**queueName:** Name of the queue.
  
**endpoint:** Endpoint of the SQS server to connect to. Omit this field to connect to AWS.


Example
-------
This example reads from a queue named 'queue_name' which is hosted on a server that's
located in the 'us-west-1' region:

    {
        "name": "AmazonSQS",
        "type": "realtimesource",
        "properties": {
            "region": "us-west-1",
            "accessID": "accessID",
            "accessKey": "accessKey",
            "queueName": "queue_name"
        }
    }

---
- CDAP Pipelines Plugin Type: realtimesource
- CDAP Pipelines Version: 1.7.0
