===================================================
Building Your First Continuuity Reactor Application
===================================================

.. .. reST Editor: .. section-numbering::
.. reST Editor: .. contents::

.. Slide Presentation HTML Generation
.. landslide: theme ../_theme/slides-generation/
.. landslide: build ../../html/

.. include:: ../_slide-fragments/continuuity_logo.rst

----

Module Objectives
=================

In this module, you will learn how to:

- Use the Continuuity Reactor ``maven`` archetype
- Build a Continuuity Reactor Application
- Deploy and run the application

----

Using the Reactor Maven Archetype
=================================

This Maven archetype generates a Reactor application Java project with the proper
dependencies and sample code as a base to start writing your own Big Data application

To generate a new project, execute the following command: 

.. sourcecode:: shell-session

    $ mvn archetype:generate \
      -DarchetypeCatalog=https://repository.continuuity.com/ \
         content/groups/releases/archetype-catalog.xml \
      -DarchetypeGroupId=com.continuuity \
      -DarchetypeArtifactId=reactor-app-archetype \
      -DarchetypeVersion=2.0.0

This can take a moment or two to start downloading...

----

Specifying the Maven Properties
=================================

- In the interactive shell that appears, specify basic properties for the new project
- After entering the *groupId* and the *artifactId*, accept the defaults for the other properties
- Enter ``Y`` to accept the results

----

Specifying the Maven Properties
=================================

For "HelloWorld":

.. sourcecode:: shell-session

	Define value for property 'groupId': : com.example
	Define value for property 'artifactId': : HelloWorld
	Define value for property 'version': 1.0-SNAPSHOT: :
	Define value for property 'package': com.example: :
	Confirm properties configuration:
	groupId: com.example
	artifactId: MyFirstBigDataApp
	version: 1.0-SNAPSHOT
	package: org.myorg
	Y: : Y

----

Building the Project
=================================

- After you confirm the settings, the directory ``HelloWorld`` is
  created under the current directory
- To build the project:

.. sourcecode:: shell-session

	$ cd HelloWorld
	$ mvn clean package

- Creates ``HelloWorld-1.0-SNAPSHOT.jar`` in the target directory
- This JAR file is a skeleton Reactor application that is ready to be edited
- When finished and compiled, deploy it by dragging and dropping it on the Reactor Dashboard

Now, what to put in that JAR file...

----

Changing The Project
====================

[DOCNOTE: FIXME! Add additional material]

[What do we want to do here?]

----

Module Summary
==============

You should now be able to:

- Use the Continuuity Reactor ``maven`` archetype
- Create a simple Continuuity Reactor Application
- Deploy and run it in the Reactor

----

Module Completed
================

`Chapter Index <return.html#m07>`__
