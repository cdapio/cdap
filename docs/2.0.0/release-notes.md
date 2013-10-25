Continuuity Reactor 2.0.0
==============

## Release Notes



**Continuuity Reactor 2.0** is graduating from public beta to being production ready system. Now, businesses and developers can build and deploy production Big Data apps using our platform. We added a collection of new features to make Continuuity Reactor 2.0 enterprise ready: MapReduce Scheduling, High Availability, Resource View and full REST API support. Developers can run an entire application in Local or Sandbox Reactor with a pre-packaged web server.

### System Requirements
  * Supported Java Version 6
  * Node JS Version greater than 0.8.16
  * CDH 4.0.x, HDP 2.x, Apache Hadoop 2.x, HBase 0.94.x
  

### Reactor Components
  * **Continuuity AppFabric**
    
    Runtime component of Reactor responsible for managing life cycle of applications running with Reactor
    
  * **Continuuity DataFabric**
    
    Data management layer of Reactor, providing Transactional support for all interactions with HBase & HDFS
    
  * **Continuuity Watchdog**
  
    Metrics and Log aggregation system support application runtime. 
    
  * **Continuuity Gateway**
  
    Extendable protocol agnostic ingestion endpoint currently supporting REST & Flume NG.
  
  * **Continuuity Dashboard**
  
    Visual Dashboard for managing application, viewing logs & metrics. 
  
### New Features
  * **Kerberos Integration**
  
    Kerberos (network authentication protocol) protects Enterprise networks against eavesdropping and replay attacks.
    Reactor now supports integrating with Hadoop Cluster using Kerberos authentication.

  * **Metrics & Logging**
  
    Application running within Continuuity Reactor now have ability to record user level metrics and also use logging to    report any application level issues. The logs and metrics are centralized are exposed through Continuuity Reactor Dashboard or available through REST APIs to be integrated with existing Enterprise assets.

    * **Log Viewer**
    
        Log Viewer allows users to investigate aggregated logs for an application either on Local or Distributed Reactor. 
    
    * **Metrics Explorer**
    
        Metrics viewer allows users to explore user defined metrics for the application. It's an exploratory view for metrics defined by users. There are RESTful APIs that allow users to retrieve these metrics to be displayed on their regular assets.

  * **Workflow**
  
    Allows scheduling and changing of MapReduce jobs. Programmatic APIs allow users to define workflow easily within IDE.

  * **Runtime Arguments**
  
    Like any UNIX like program, Reactor applications now have the ability to pass run time arguments during start of the application. The arguments can be saved by the Reactor. REST APIs are available to save and retrieve run time arguments.

  * **RESTful APIs**
  
     Reactor 2.0.0 now exposes application lifecycle management, metrics & logging & meta data for all applications running with Reactor. This ability allows Enterprises to integrate Reactor into their regular workflow of Enterprise assets.  
  
  * **Resource Specification**
  
    Developers can now specify the resources in terms of CPU & Memory required for running applications on YARN Cluster. 
  
  * **Cluster Resource Views**
  
    Operators of Reactor now have a way to look at resources being used on cluster and also drill down into application
    and it's components to see how much resources are being used. It's an easy way to visualize what's available on cluster. This information is also available through RESTful APIs   
   
  * **New Datasets**
    Better and cleaner Dataset APIs are introduced with this release along with some useful Dataset to build new application. Can be used in MapReduce, Flow & Procedures.
    
    * **ObjectStore**
    
      Daatset allows to store objects of a particular class into a table. Plain java class, Parametrized and static inner class are supported. Can be used in MapReduce, Flow & Procedures.
      
    * **IndexedObjectStore**
    
      This dataset is an extension of ObjectStore that supports access to object via indices. This can be used in conjuction with ObjectStore for indexing objects. Can be used in MapReduce, Flow & Procedures.
      
    * **MultiObjectStore**
    
      This dataset supports storing one or more objects for the same key. Multiple objects can be stored using different
      column names for each object. Can be used in MapReduce, Flow & Procedures.
      
    * **IndexedTable**
    
      This dataset allows data to be accessed by secondary key. For e.g. This dataset can be used for index full text index of content. Can be used in MapReduce, Flow & Procedures.
    
    * **TimeseriesTable**
    
      This dataset allows timeseries at scale. It can be used in MapReduce, Flow & Procedures.
    
  * **Web Application**
  
    Developers now have the ability to deploy a complete application including frontend to be deployed on the Reactor. 
  
  * **High Availability**
  
    Reactor 2.0.0 brings stability as well as all the components of the Reactor can be configured to be highly available. 
  
  * **Power Pack**
  
    Power pack is a extension library that consists of extensions that we thought are helpful for other developers. Extensions are based on feedback from our customers or our POC with customers. Version 1.0.0 of Power Pack is available to download from Continuuity Repository.
    
    * **Cube**
    
      Persistant Streaming Online analytical processing extension support real time processing and serving for aggregation on multi dimensions.
      
    * **External Process**
    
      Developers are now able to use the executable of their choice to integrate with our real time system (BigFlow).
      A high level abstraction that makes is easy to integrate is available as part of this release.
      
  * **Unit Testing Framework**
  
    Developers can now build and unit test any part of Reactor application (Flow, MapReduce, Procedure, Workflow) from with their IDE. 
  
  * **IDE based debuggability on Singlenode**
  
    Developers can start Local reactor in debug mode and test their application within singlenode by connecting to it from their favourite IDE. 
    
  * **Improved singlenode performance**
  
    Performance of singlenode has been drammaticaly improved. Running a huge application with Local Reactor is now possible. 
    
  * **Reactor application template using maven archetype**
  
    In order to make it easy for developer to build new Reactor application, a maven archetype is supported and downloadable from Continuuity repository. 
      
  * **Virtual Box Local Reactor (for windows user)** 
  
    For Windows users, we now have a Virtual box that they can download to get started Local Reactor on their very own windows machine. 
   

### Known Issues

   * YARN Application Master Single Point of Failure.
   
     Even though Reactor components are highly available,  YARN Application master SPOF is still a issue. This is being worked on and will be available in future releases.
   
   * Kerberos token refresh is not available
   
     Kerberos acquired tokens are not refreshed. So long running application might fail when running on Kerberos enabled hadoop clusters. Bug Fix release 2.0.1 will be soon available that will resolve this issue. 