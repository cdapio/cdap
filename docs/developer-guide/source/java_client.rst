.. :author: Cask Data, Inc.
   :description: A Java Client for Interacting With the Cask Data Application Platform 

==============================================
Cask Data Application Platform Java Client API
==============================================

Introduction
============

The Cask Data Application Platform (CDAP) Java Client API provides methods for interacting
with CDAP from Java applications.

Maven Dependency
================

.. highlight:: console

To use the Java Client API in your project, add this Maven dependency::

  <dependency>
    <groupId>co.cask.cdap</groupId>
    <artifactId>client</artifactId>
    <version>${cdap.version}</version>
  </dependency>

.. highlight:: java

Components
==========

The Java Client API allows you to interact with these CDAP components:

- `ApplicationClient`_: interacting with applications
- `DatasetClient`_: interacting with Datasets
- `DatasetModuleClient`_: interacting with Dataset Modules
- `DatasetTypeClient`_: interacting with Dataset Types
- `MetricsClient`_: interacting with Metrics
- `MonitorClient`_: monitoring System Services
- `ProcedureClient`_: interacting with Procedures
- `ProgramClient`_: interacting with Flows, Procedures, MapReduce Jobs, User Services, and Workflows
- `QueryClient`_: querying Datasets
- `ServiceClient`_: interacting with User Services
- `StreamClient`_: interacting with Streams

Sample Usage
============

ApplicationClient
-----------------
::

  // Interact with the CDAP instance located at example.com, port 10000
  ClientConfig clientConfig = new ClientConfig("example.com", 10000);

  // Construct the client used to interact with CDAP
  ApplicationClient appClient = new ApplicationClient(clientConfig);

  // Fetch the list of applications
  List<ApplicationRecord> apps = appClient.list();

  // Deploy an application
  File appJarFile = ...;
  appClient.deploy(appJarFile);

  // Delete an application
  appClient.delete("Purchase");

  // List programs belonging to an application
  appClient.listPrograms("Purchase");


DatasetClient
-------------
::

  // Interact with the CDAP instance located at example.com, port 10000
  ClientConfig clientConfig = new ClientConfig("example.com", 10000);

  // Construct the client used to interact with CDAP
  DatasetClient datasetClient = new DatasetClient(clientConfig);

  // Fetch the list of Datasets
  List<DatasetSpecification> datasets = datasetClient.list();

  // Create a Dataset
  datasetClient.create("someDataset", "someDatasetType");

  // Delete a Dataset
  datasetClient.delete("someDataset");


MetricsClient
-------------
::

  // Interact with the CDAP instance located at example.com, port 10000
  ClientConfig clientConfig = new ClientConfig("example.com", 10000);

  // 
  JsonObject metric = metricsClient.getMetric("user", "/apps/HelloWorld/flows", 
                                              "process.events", "aggregate=true");

