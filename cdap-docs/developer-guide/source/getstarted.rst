.. :author: Cask Data, Inc.
   :description: Getting Started with Cask Data Application Platform
         :copyright: Copyright © 2014 Cask Data, Inc.

===================================================
Getting Started with Cask Data Application Platform
===================================================

This chapter is a guide to help you get started with CDAP and prepares you to be familiar with the environment, At the
end of this topic you will have CDAP up and running on your platform, you would learn how to develop, deploy and play with
CDAP  with the help of the sample CDAP App provided in this chapter.

Before Getting Started

The minimum requirements to run CDAP applications are only three,
- `JDK 6 or JDK 7 <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`__ (required to run CDAP; note that $JAVA_HOME should be set)
- `Node.js 0.8.16+ <http://nodejs.org>`__ (required to run the CDAP UI)
- `Apache Maven 3.0+ <http://maven.apache.org>`__ (required to build CDAP applications)

Start Using CDAP
================
You can Download the CDAP tarball at <link>

CDAP is also available in Maven under the following identifiers
  co.cask.cdap:cdap

Setting up Development Environment
==================================

  1 To generate a sample CDAP application, you would use the maven archetype
  ``TODO : Update Archetype ``
  ``mvn archetype:generate -   DarchetypeCatalog=https://repository.continuuity.com/content/groups/releases/archetype-catalog.xml -DarchetypeGroupId=com.continuuity -DarchetypeArtifactId=reactor-app-archetype -DarchetypeVersion=2.3.0``

  2 To setup the CDAP application development environment, you need to import the generated pom file in your IDE.
     Now you are all set to start developing your first CDAP application.

  3 CDAP comes with a set of Prebuilt specialized CDAP apps, that showcases the power and simplicity of developing
    Big Data Apps using CDAP. We can see how you can deploy one of the example app and experience what you can do with it.

  This is an example Javadoc link to AbstractFlowlet_
    Will the link still work?
  .. _AbstractFlowlet: javadocs/co/cask/cdap/api/mapreduce/MapReduce.html










