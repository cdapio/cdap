
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

