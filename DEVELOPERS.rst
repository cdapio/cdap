=================================
CDAP Contributor Quickstart Guide
=================================

This guide outlines the steps to setup your system for contributing to CDAP.


Prerequisites
=============

- Java 7+ SDK
- Maven 3.1+
- Git


Setup for IntelliJ IDEA
=======================

First, open the CDAP project in IntelliJ IDEA: ``Select File > Open... > cdap/pom.xml``

Then, configure a run configuration to run CDAP Sandbox:

- Select Run > Edit Configurations...
- Add a new "Application" run configuration
- Set "Main class" to be ``co.cask.cdap.StandaloneMain``
- Set "VM options" to ``-Xmx1024m`` (for in-memory Map/Reduce jobs)
- Click "OK"

You can now use this run configuration to start an instance of CDAP Sandbox.


Build and Run CDAP Sandbox in a Docker Container
======================================================

These instructions assume the user has a working installation of Docker and a working
understanding of Docker behavior and commands.

- Obtain a fresh copy of the CDAP (GitHub) repo::

    git clone git@github.com:caskdata/cdap.git

- Build the Docker image: (from the cdap/ root directory)::

    docker build caskdata/cdap-standalone .

- Run a Docker container from the new image::

    docker run -d -p 11011:11011 -p 11015:11015 caskdata/cdap-sandbox

You now have a new Docker container running with CDAP Sandbox.
