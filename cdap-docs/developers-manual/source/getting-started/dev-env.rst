.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _dev-env:

============================================
Development Environment Setup
============================================

.. this file is included in others; titles need to be +

.. highlight:: console

Creating an Application
----------------------------------

When writing a CDAP Application, it's best to use an integrated development environment that
understands the application interface to provide code-completion in writing interface
methods.

The best way to start developing a CDAP application is by using the Maven archetype:

.. container:: highlight

  .. parsed-literal::
  
    |$| mvn archetype:generate \\
          -DarchetypeGroupId=co.cask.cdap \\
          -DarchetypeArtifactId=cdap-app-archetype \\
          -DarchetypeVersion=\ |release|

This creates a Maven project with all required dependencies, Maven plugins, and a simple
application template for the development of your application. You can import this Maven project into your preferred IDE—such as 
`IntelliJ <https://www.jetbrains.com/idea/>`__ or 
`Eclipse <https://www.eclipse.org/>`__—and start developing your first
CDAP application.

Using IntelliJ
----------------------------------

1. Open `IntelliJ <https://www.jetbrains.com/idea/>`__ and import the Maven project.
#. Go to menu *File -> Import Project*...
#. Select the ``pom.xml`` in the Maven project's directory.
#. Select the *Import Maven projects automatically* and *Automatically download: Sources, Documentation*
   boxes in the *Import Project from Maven* dialog.
#. Click *Next*, complete the remaining dialogs, and the new CDAP project will be created and opened.

Using Eclipse
----------------------------------

1. In your `Eclipse <https://www.eclipse.org/>`__ installation, make sure you have the
   `m2eclipse <http://m2eclipse.sonatype.org>`__ plugin installed.
#. Go to menu *File -> Import*
#. Enter *maven* in the *Select an import source* dialog to filter for Maven options.
#. Select *Existing Maven Projects* as the import source.
#. Browse for the Maven project's directory.
#. Click *Finish*, and the new CDAP project will be imported, created and opened.
