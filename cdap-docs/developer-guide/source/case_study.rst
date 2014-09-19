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

The Wise application - Web Insights Engine application - uses the following CDAP constructs to analyze web server logs:

- **Stream**: Ingests log data in real-time
- **Flow**: Applies transformation on log data in real-time
- **Dataset**: Stores log analytics according to custom data pattern
- **MapReduce**: Processes chunks of log data in batch jobs to extract deeper information
- **Service**: Exposes APIs to query Datasets
- **Explore**: Executes Ad-hoc queries over Datasets


Running Wise
============
Building and running Wise is easy. Simply download the source tarball from the following location::

  url

You can then expand the tarball and build the application by executing::

  $ tar -xzf cdap-wise-0.1.0.tar.gz
  $ cd cdap-wise-0.1.0
  $ mvn package

To deploy the application, first make sure CDAP standalone (*link*) is running, then execute::

  $ bin/deploy.sh

Overview of Wise
================
Throughout this case study, we are going to present and explain the different constructs that the Wise application
uses. Let's first have a look at a diagram showing a overview of the Wise application's architecture.