Release Notes
==============

## Reactor 2.0.0

Ubuntu 13.10 introduces the first release of Ubuntu for phones and Ubuntu Core for the new 64-bit 
ARM systems (the "arm64" architecture, also known as AArch64 or ARMv8). In addition to these flagship 
features there are major updates throughout: OpenStack 2013.2 Havana, Apache 2.4, LXC 1.0, Puppet 3, 
improved AppArmor confinement, and many other upgrade.


### System Requirements
  * Supported Java Version 6
  * Node JS Version greater than 0.8.16
  * CDH 4.0.x, HDP 2.x, Apache Hadoop 2.x, HBase 0.94.x

### New Features
  * Kerberos Integration
  * Metrics & Logging
    * Log Viewer
    * Metrics Explorer
    * API
  * Workflow
  * Runtime Arguments
  * RESTful APIs
  * Cluster Resource Views
  * Resource Specification
  * New Datasets
    * ObjectStore
    * IndexedObjectStore
    * MultiObjectStore
    * IndexedTable
    * TimeseriesTable
  * Web Application
  * High Availability
  * Power Pack
    * Cube
    * External Process
  * Unit Testing Framework
  * IDE based debuggability on Singlenode
  * Improved singlenode performance
  * Reactor application template using maven archetype
  * Improved example applications
   
### API
  * Flow API
    * Tick
    * Properties
    * DisableTransaction
  * Dataset API

### Known Issues
   * YARN Application Master Single Point of Failure
   * KerberOS token refresh is not available
