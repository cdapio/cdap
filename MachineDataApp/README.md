---
Machine Data collection example on the Continuuity AppFabric

This project contains the code, build files, to run the Machine Data example. It also
contains command line drivers to generate events and read metrics from procedure and
query.
--- 

DIRECTORY LAYOUT

bin/	 Command-line clients to generate metrics events
build.xml  Maven project file (see below for some pointers)
src/     The source code for the entire project and tests

GETTING STARTED

First, start the App Fabric from using the developer's suite. 


BUILDING PROJECT JARS

The project has no external dependencies that aren't included with the App Fabric. To build
and deploy on the AppFabric (should be started): 

	ant deploy 

This will generate the MachineDataApp.jar in the root directory ant deploy to the local AppFabric 
	
To compile only: 

	ant compile 

To compile and package only: 

	ant dist

This will generate MachineDataApp.jar in the root directory. 

DRIVER scripts

To start generating events, run:

	bin/start_all.sh <num_metrics> <rate in seconds> 
	
This will generate cpu, disk and memory metrics, specify the number of metrics to generate and 
the interval in seconds (use decimals for milliseconds). 

To generate a single type of metrics: 

	bin/collect_<cpu, memory, disk>.sh <num_metrics> <rate in seconds> 
	
To query sample data:

	bin/query_sample.sh

This will produce 3 files output JSON files, cpu.json, memory.json, disk.json which which
contains all metrics collected in the last hour. 
	 

 

