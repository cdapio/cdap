.. :author: Cask Data, Inc.
   :description: Index document
   :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Development Environment Setup
============================================

.. highlight:: console

Creating an Application
=======================

The best way to start developing a CDAP application is by using the Maven archetype::

  $ mvn archetype:generate \
    -DarchetypeGroupId=co.cask.cdap \
    -DarchetypeArtifactId=cdap-app-archetype \
    -DarchetypeVersion=2.5.0

This creates a Maven project with all required dependencies, Maven plugins, and a simple
application template for the development of your application. You can import this Maven project
into your preferred IDE—such as Eclipse or IntelliJ—and start developing your first
CDAP application.
