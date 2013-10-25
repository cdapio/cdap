Continuuity Reactor 2.0.0
==============

## Release Notes



**Continuuity Reactor 2.0** is the graduation of Reactor from public beta to a production ready system. Now businesses and developers can build and deploy production Big Data applicationss using our platform. We added a collection of new features to make Continuuity Reactor 2.0 enterprise ready: MapReduce Scheduling, High Availability, Application Resource View and full REST API support. Developers can run an entire application in a Local or Sandbox Reactor with a pre-packaged web server.

### System Requirements
  * Java Version 6
  * Node JS Version greater than 0.8.16
  * CDH 4.0.x, HDP 2.x, Apache Hadoop 2.x, HBase 0.94.x
  

### Reactor Components
  * **Continuuity AppFabric**
    
    The runtime component of Reactor responsible for managing the life cycle of applications running within Reactor.
    
  * **Continuuity DataFabric**
    
    The data management layer of Reactor, providing transactional support for all interactions with HBase & HDFS.
    
  * **Continuuity Watchdog**
  
    Metrics and log aggregation system that supports application runtime. 
    
  * **Continuuity Gateway**
  
    An extendable protocol agnostic ingestion endpoint currently supporting REST & Flume NG.
  
  * **Continuuity Dashboard**
  
    A visual Dashboard for managing application, viewing logs & metrics. 
  
### New Features
  * **Kerberos Integration**
  
    Kerberos (network authentication protocol) protects Enterprise networks against eavesdropping and replay attacks.
    Reactor now supports integrating with Hadoop cluster using Kerberos authentication.

  * **Metrics & Logging**
  
    Applications running within Continuuity Reactor now have the ability to record user level metrics and user logging to report any application level issues. The logs and metrics are centralized and exposed through Continuuity Reactor Dashboard or available through REST APIs for integration with existing Enterprise assets.

    * **Log Viewer**
    
        Log Viewer allows users to investigate aggregated logs for an application on either a Local or Distributed Reactor. 
    
    * **Metrics Explorer**
    
        Metrics Explorer allows users to explore user defined metrics for their application. There are RESTful APIs that allow users to retrieve these metrics, which can be displayed on their regular assets.

  * **Workflow**
  
    Workflow allows scheduling of and changing MapReduce jobs. Programatic APIs allow users to define workflows easily within their IDE.

  * **Runtime Arguments**
  
    Like any UNIX like program, Reactor applications now have the ability to pass runtime arguments at the start of their application. The arguments can be saved by the Reactor. REST APIs are available to save and retrieve runtime arguments.

  * **RESTful APIs**
  
     Reactor 2.0.0 now exposes application lifecycle management, metrics & logging, and meta data for all applications running within Reactor. This ability allows Enterprises to integrate Reactor into their regular workflow of Enterprise assets.  
  
  * **Resource Specification**
  
    Developers can now specify resources in terms of CPU and memory required to run applications on a YARN Cluster. 
  
  * **Cluster Resource Views**
  
    Reactor operators now have a way to look at resources being used on cluster and also have the ability to drill down into an application and its components to see how many resources are being used. It's an easy way to visualize what is available on cluster. This information is also available through RESTful APIs.   
   
  * **New Datasets**
    Better and cleaner Dataset APIs are introduced with this release, along with some useful Datasets to build new application. These can be used with MapReduce, Flows and Procedures.
    
    * **ObjectStore**
    
      This Dataset allows storing objects of a particular class into a table. Plain java class, parametrized and static inner class are supported. This can be used in MapReduce, Flow and Procedures.
      
    * **IndexedObjectStore**
    
      This dataset is an extension of ObjectStore that supports access to object via indices. This can be used in conjuction with ObjectStore for indexing objects. Can be used in MapReduce, Flow and Procedures.
      
    * **MultiObjectStore**
    
      This dataset supports storing one or more objects for the same key. Multiple objects can be stored using different
      column names for each object. Can be used in MapReduce, Flow and Procedures.
      
    * **IndexedTable**
    
      This dataset allows data to be accessed by secondary key. This dataset can be used to index full text index of content. Can be used in MapReduce, Flow and Procedures.
    
    * **TimeseriesTable**
    
      This dataset allows timeseries at scale. It can be used in MapReduce, Flow and Procedures.
    
  * **Web Application**
  
    Developers now have the ability to deploy a complete application ,including the frontend, to be deployed on Reactor. 
  
  * **High Availability**
  
    Reactor 2.0.0 has greater stability. All Reactor component can be configured to be highly available. 
  
  * **Power Pack**
  
    Power pack is a extension library that we thought would helpful for developers. Extensions are based on feedback from our customers. Version 1.0.0 of Power Pack is available to download from Continuuity Repository.
    
    * **Cube**
    
      Cube is a Persistant Streaming Online analytical processing extension that supports real time processing and serving for aggregation on multi dimensions.
      
    * **External Process**
    
      Developers are now able to use the executable of their choice to integrate with our real time system (BigFlow).
      A high level abstraction that makes is easy to integrate is available as part of this release.
      
  * **Unit Testing Framework**
  
    Developers can now build and unit test any part of Reactor applications (Flow, MapReduce, Procedure, Workflow) from with their IDE. 
  
  * **IDE based debuggability on Singlenode**
  
    Developers can start a Local reactor in debug mode and test their application within singlenode by connecting to it from their favourite IDE. 
    
  * **Improved singlenode performance**
  
    Performance of singlenode has been drammaticaly improved. Running a large application within Local Reactor is now possible. 
    
  * **Reactor application template using maven archetype**
  
    In order to make it easy for developer to build new Reactor application, a maven archetype is supported and downloadable from Continuuity repository. 
      
  * **Virtual Box Local Reactor (for windows user)** 
  
    For Windows users, we now have a Virtual box that they can download to get started Local Reactor on their own Windows machine. 
   

### Known Issues

   * YARN Application Master Single Point of Failure.
   
     Even though Reactor components are highly available,  YARN Application master SPOF is still a issue. This is being worked on and will be available in future releases.
   
   * Kerberos token refresh is not available.
   
     Kerberos acquired tokens are not refreshed. Long running application might fail when running on Kerberos enabled Hadoop clusters. Bug Fix release 2.0.1 will be available soon  and  will resolve this issue. 
