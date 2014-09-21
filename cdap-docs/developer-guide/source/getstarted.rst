.. :author: Cask Data, Inc.
   :description: Getting Started with Cask Data Application Platform
         :copyright: Copyright © 2014 Cask Data, Inc.

===================================================
Getting Started with Cask Data Application Platform
===================================================

This chapter is a guide to help you get started with CDAP and prepares you to be familiar with the environment, At the
end of this topic you will have CDAP up and running on your platform, you would learn how to develop, deploy and play with
CDAP  with the help of the sample CDAP App provided in this chapter.

CDAP is built keeping the developer's in mind. The CDAP SDK provides ease of use for java developers to adpot big-data paradigm as well as
its easily extensible to satisfy the needs for existing big-data developers. The CDAP SDK

Before Getting Started

The minimum requirements to run CDAP applications are only three,
 - `JDK 6 or JDK 7 <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`__ (required to run CDAP; note that $JAVA_HOME should be set)
 - `Node.js 0.8.16+ <http://nodejs.org>`__ (required to run the CDAP UI)
 - `Apache Maven 3.0+ <http://maven.apache.org>`__ (required to build CDAP applications)

Getting CDAP
============
CDAP is available as a source tarball and binary on the Downloads section of Cask Website. If you want to get started quickly, the binary is the easiest way to get started.


Building from Source
....................

**Check out the source** ::

    $ git clone https://github.com/caskco/cdap cdap
    $ cd cdap

**Compile the Project** ::

  $ mvn clean package -DskipTests -P examples -pl cdap-examples -am -amd && mvn package -pl cdap-standalone -am -DskipTests -P dist,release

.. note:: Recommended memory settings for maven: export MAVEN_OPTS="-Xms512m -Xmx1024m -XX:PermSize=256m -XX:MaxPermSize=512m"

If you are a user, you would want the binary distribution ::

  $ cp cdap-standalone/target/cdap-sdk-<version>.zip .
  $ tar -zxvf cdap-sdk-<version>.zip
  $ cd cdap-sdk-<version>

**Running CDAP** ::

    $ ./bin/cdap.sh start (If you are using Windows, use the batch script to start)

  Once CDAP is started successfully, you can see the CDAP web UI running at localhost:9999, you can head there to deploy sample example apps and experience CDAP.

For Developers
..............

To generate a sample CDAP application, you would use the maven archetype ::

   ``mvn archetype:generate -   DarchetypeCatalog=https://repository.continuuity.com/content/groups/releases/archetype-catalog.xml -DarchetypeGroupId=com.continuuity -DarchetypeArtifactId=reactor-app-archetype -DarchetypeVersion=2.3.0``

To setup the CDAP application development environment, you need to import the generated pom file in your IDE
Now you are all set to start developing your first CDAP application.
