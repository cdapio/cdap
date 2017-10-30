.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

.. _dev-env:

=============================
Development Environment Setup
=============================

.. this file is included in others; titles need to be "-" rather than "="

.. highlight:: console

Creating an Application
-----------------------

When writing a CDAP application, it's best to use an integrated development environment
(IDE) that understands the application interface and provides code-completion in writing
interface methods.

The best way to start developing a CDAP application is by using the Maven archetype:

.. ifconfig:: snapshot_version

  .. tabbed-parsed-literal::

    $ mvn archetype:generate \
        -DarchetypeGroupId=co.cask.cdap \
        -DarchetypeArtifactId=cdap-app-archetype \
        |archetype-repository| \
        |archetype-version| \
        -DartifactId=myExampleApp \
        -DgroupId=org.example.app

.. ifconfig:: not snapshot_version

  .. tabbed-parsed-literal::

    $ mvn archetype:generate \
        -DarchetypeGroupId=co.cask.cdap \
        -DarchetypeArtifactId=cdap-app-archetype \
        |archetype-version| \
        -DartifactId=myExampleApp \
        -DgroupId=org.example.app

This creates a Maven project with all required dependencies, Maven plugins, and a simple
application template for the development of your application (``myExampleApp``). You can
import this Maven project into your preferred IDE |---| such as `IntelliJ
<https://www.jetbrains.com/idea/>`__ or `Eclipse <https://www.eclipse.org/>`__ |---| and
start developing your first CDAP application.

For an application that contains a MapReduce program, set the ``archetypeArtifactId`` to
``cdap-mapreduce-archetype``; for Spark, use either ``cdap-spark-java-archetype`` or
``cdap-spark-scala-archetype``.

**Note:** Replace the *artifactId* (``myExampleApp``) and *groupId* parameters
(``org.example.app``) with your own app name and organization, but the *groupId* must not
be replaced with ``co.cask.cdap``.

Complete examples for each archetype:

.. tabbed-parsed-literal::
  :tabs: "CDAP Application","MapReduce Program","Spark Program (Java)","Spark Program (Scala)"
  :dependent: dev-env-archetype

  .. CDAP Application

  $ mvn archetype:generate -DarchetypeGroupId=co.cask.cdap -DarchetypeArtifactId=cdap-app-archetype |archetype-repository-version|

  .. MapReduce Program

  $ mvn archetype:generate -DarchetypeGroupId=co.cask.cdap -DarchetypeArtifactId=cdap-mapreduce-archetype |archetype-repository-version|

  .. Spark Program (Java)

  $ mvn archetype:generate -DarchetypeGroupId=co.cask.cdap -DarchetypeArtifactId=cdap-spark-java-archetype |archetype-repository-version|

  .. Spark Program (Scala)

  $ mvn archetype:generate -DarchetypeGroupId=co.cask.cdap -DarchetypeArtifactId=cdap-spark-scala-archetype |archetype-repository-version|

When prompted, complete the values for *groupId* and *artifactId* parameters. Enter for the *groupId* parameter your
own organization; it must not be replaced with ``co.cask.cdap``. (The *version* and *package* parameters can be either
specified or you can use the Maven defaults.)

Maven supplies a guide to the naming convention used above at https://maven.apache.org/guides/mini/guide-naming-conventions.html.

Using IntelliJ
--------------
1. Open `IntelliJ <https://www.jetbrains.com/idea/>`__ and import the Maven project by:

   - If at the starting IntelliJ dialog, click on *Import Project*; or
   - If an existing project is open, go to the menu item *File -> Open...*

#. Navigate to and select the ``pom.xml`` in the Maven project's directory.
#. In the *Import Project from Maven* dialog, select the *Import Maven projects automatically* and *Automatically
   download: Sources, Documentation* boxes.
#. Click *Next*, complete the remaining dialogs, and the new CDAP project will be created and opened.

Using Eclipse
-------------
1. In your `Eclipse <https://www.eclipse.org/>`__ installation, make sure you have the
   `m2eclipse <http://m2eclipse.sonatype.org>`__ plugin installed.
#. Go to menu *File -> Import*
#. Enter *maven* in the *Select an import source* dialog to filter for Maven options.
#. Select *Existing Maven Projects* as the import source.
#. Browse for the Maven project's directory.
#. Click *Finish*, and the new CDAP project will be imported, created and opened.

Running CDAP from within an IDE
-------------------------------
As CDAP is an open source project, you can download the source, import it into an IDE,
then modify, build, and run CDAP.

To do so, follow these steps:

1. Install all the :ref:`prerequisite system requirements <system-requirements>` for CDAP development.

#. Either clone the CDAP repo or download a ZIP of the source:

   - Clone the CDAP repository using |git-clone-command|

   - Download the source as a ZIP from |source-link| and unpack the ZIP in a suitable location

#. In your IDE, install the Scala plugin (for
   `IntelliJ <https://confluence.jetbrains.com/display/SCA/Scala+Plugin+for+IntelliJ+IDEA>`__
   or `Eclipse <http://scala-ide.org>`__) as there is Scala code in the project.
#. Open the CDAP project in the IDE as an existing project by finding and opening the ``cdap/pom.xml``.
#. Resolve dependencies: this can take quite a while, as there are numerous downloads required.
#. In the case of IntelliJ, you can create a run configuration to run CDAP Sandbox:

   1. Select ``Run > Edit`` Configurations...
   #. Add a new "Application" run configuration.
   #. Set "Main class" to be ``co.cask.cdap.StandaloneMain``.
   #. Set "VM options" to ``-Xmx1024m -XX:MaxPermSize=128m`` (for in-memory MapReduce jobs).
   #. Click "OK".
   #. You can now use this run configuration to start an instance of CDAP Sandbox.

This will allow you to start CDAP and access it from either the command line (:ref:`CLI <cli>`)
or through the :ref:`HTTP RESTful API <http-restful-api>`. To start the CLI, you can either start
it from a shell using the ``cdap`` script or run the ``CLIMain`` class from the IDE.

If you want to run and develop the UI, you will need to follow additional instructions in the |ui-read-me|.
