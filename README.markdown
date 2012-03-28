*Continuuity Performance Repository*
====================================
*Copyright (c) 2012 to Continuuity Inc. All rights reserved.*

## Queue Performance

Continuuity Queues are essentially the basic building blocks for building 
real time big data applications. Continuuity Queues are essentially built 
on top of HBase (Sharded key-value store). Understanding the performance 
characteristics of different implementation of the Queues under different 
workloads is critically important to us.

NOTE: Current implementation only supports connecting to local hbase instance. 
Soon we will be adding the capability to connect to any hbase instance. 

## How to

In order to run queue performance, please follow the steps below

 1. git clone https://<user>@github.com/continuuity/performance
 2. cd performance
 3. mvn clean package
 4. Setup hbase on local machine
 5. bin/queue-harness <properties-file> <test-name>
 6. bin/analyze <path-to-raw-results>

## Configuration file

* **storage.engine**

  Specifies the storage engine to be used for the test. Currently supports
  hbase and memory

* **result.dir**

  Specifies the directory where the results would be stored.

* **message.size**

  Specifies the size of the message for the test

* **message.count**

  Specifies the number of messages that will be generated and pushed 
  through the queue for tests

* **queue.count**

  Specifies the number of queues to be used during the test.

* **producer.count**

  Specifies number of producers that will be part of the test. 
  Number of producers that will be assigned per queue is 
  mod(producer.count, queue.count)

* **consumer.count**

  Specifies number of consumers that will be part of the test.
