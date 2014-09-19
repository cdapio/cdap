.. :author: Cask Data, Inc.
   :description: Advanced Cask Data Application Platform Features
   :copyright: Copyright © 2014 Cask Data, Inc.

===========================
CDAP Application Case Study
===========================

**Web Analytics using CDAP**

Introduction
============
... *More to come* ...

The Wise application uses the following CDAP constructs to analyze web server logs:

- **Stream**: Ingests log data in real-time
- **Dataset**:
- **Flow**:
- **MapReduce**:
- **Service**:
- **Explore**:


Running Wise
============
Building and running Wise is easy. Simply download the source tarball from the following location::

  url

You can then expand the tarball and build the application by executing::

  $ tar -xzf cdap-wise-0.1.0.tar.gz
  $ cd cdap-wise-0.1.0
  $ mvn package

To deploy the application, first make sure cdap standalone (*link*) is running, then execute::

  $ bin/deploy.sh

