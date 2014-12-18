
# CDAP Contributor Quickstart Guide

This guide outlines the steps to setup your system for contributing to CDAP.

## Prerequisites
* Java 6+ SDK
* Maven
* Git

## Setup for IntelliJ IDEA

First, open the CDAP project in IntelliJ IDEA: Select File > Open... > cdap/pom.xml

Then, configure a run configuration to run CDAP Standalone:

* Select Run > Edit Configurations...
* Add a new "Application" run configuration
* Set "Main class" to be `co.cask.cdap.StandaloneMain`
* Set "VM options" to `-Xmx1024m` (for in-memory Map/Reduce jobs)
* Click "OK"

You can now use this run configuration to start an instance of CDAP Standalone.

## Build and Run CDAP Standalone in a Docker container

Prerequisite: Docker

* Obtain a fresh copy of the CDAP (GitHub) repo:
```
git clone git@github.com:caskdata/cdap.git
```

* Build the Docker image: (from the cdap/ root directory)
```
docker build caskdata/cdap-standalone .
```

* Run a Docker container from the new image:
```
docker run -d -p 9999:9999 -p 10000:10000 caskdata/cdap-standalone
```

You now have a new Docker container running with CDAP Standalone.
