=================
 Java Client API
=================

Introduction
============

The Java Client API provides methods for interacting with Reactor from Java applications.

Maven Dependency
================

To use the Java Client API in your project, add this Maven dependency::

  <dependency>
    <groupId>com.continuuity</groupId>
    <artifactId>client</artifactId>
    <version>${reactor.version}</version>
  </dependency>

Components
==========

The Java Client API allows you to interact with these Reactor components:

- **ApplicationClient:** interacting with applications
- **DatasetClient:** interacting with Datasets
- **DatasetModuleClient:** interacting with Dataset Modules
- **DatasetTypeClient:** interacting with Dataset Types
- **MetricsClient:** interacting with Metrics
- **MonitorClient:** monitoring System Services
- **ProcedureClient:** interacting with Procedures
- **ProgramClient:** interacting with Flows, Procedures, MapReduce jobs, User Services, and Workflows
- **QueryClient:** querying Datasets
- **ServiceClient:** interacting with User Services
- **StreamClient:** interacting with Streams

Sample Usage
============

ApplicationClient
-----------------

::

  // Interact with the Reactor instance located at example.com, port 10000
  ReactorClientConfig clientConfig = new ReactorClientConfig("example.com", 10000);

  // Construct the client used to interact with Reactor
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

  // Interact with the Reactor instance located at example.com, port 10000
  ReactorClientConfig clientConfig = new ReactorClientConfig("example.com", 10000);

  // Construct the client used to interact with Reactor
  DatasetClient datasetClient = new DatasetClient(clientConfig);

  // Fetch list of Datasets
  List<DatasetSpecification> datasets = datasetClient.list();

  // Create a Dataset
  datasetClient.create("someDataset", "someDatasetType");

  // Delete a Dataset
  datasetClient.delete("someDataset");
