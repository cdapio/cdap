===================================================
Building A Continuuity Reactor Application
===================================================

.. reST Editor: .. section-numbering::
.. reST Editor: .. contents::

.. rst2pdf: CutStart
.. landslide: theme ../_theme/slides-generation/
.. landslide: build ../../html/

.. include:: ../_slide-fragments/continuuity_logo_copyright.rst

.. |br| raw:: html

   <br />
.. rst2pdf: CutStop

.. rst2pdf: config ../../../developer-guide/source/_templates/pdf-config
.. rst2pdf: stylesheets ../../../developer-guide/source/_templates/pdf-stylesheet
.. rst2pdf: build ../../pdf/
.. rst2pdf: .. |br|  unicode:: U+0020 .. space

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
      -DarchetypeCatalog=http://tinyurl.com/ndoa5l2 \
      -DarchetypeGroupId=com.continuuity \
      -DarchetypeArtifactId=reactor-app-archetype \
      -DarchetypeVersion=2.1.0

This can take a moment or two to start downloading...

----

Specifying the Maven Properties
=================================

- In the interactive shell that appears, specify basic properties for the new project
- After entering the *groupId* and the *artifactId*, accept the defaults for the other
  properties
- *groupId* could be ``com.example``
- *artifactId* (the project name) could be ``BigDataApp``
- Enter ``Y`` or press ``Enter`` to accept the default or results

----

Specifying the Maven Properties
=================================

Entering *groupId* as ``com.example`` and  *artifactId* as ``BigDataApp``:

.. sourcecode:: shell-session

	Define value for property 'groupId': : com.example
	Define value for property 'artifactId': : BigDataApp
	Define value for property 'version': 1.0-SNAPSHOT: :
	Define value for property 'package': com.example: :
	Confirm properties configuration:
	groupId: com.example
	artifactId: BigDataApp
	version: 1.0-SNAPSHOT
	package: com.example
	Y: : Y

----

Building the Project
=================================

- After you confirm the settings, the directory ``BigDataApp`` is
  created under the current directory
- To build the project:

.. sourcecode:: shell-session

	$ cd BigDataApp
	$ mvn clean package

- Creates ``BigDataApp-1.0-SNAPSHOT.jar`` in the target directory
- This JAR file is a skeleton Reactor application that is ready to be edited
- When finished and compiled, deploy it by dragging and dropping it on the Reactor Dashboard

Now, what to put in that JAR file...

----

Opening in IntelliJ
=================================

- You can open the resulting project in IntelliJ
- From within IntelliJ, ``File > Open`` and navigate to the 
  ``pom.xml`` file within ``BigDataApp`` directory
- IntelliJ will then open the maven project
- The source code will be in ``src > main > java > com.example > WordCountApp``

----

Module Summary
==============

You should now be able to:

- Use the Continuuity Reactor ``maven`` archetype
- Create a simple Continuuity Reactor Application
- Open it in an IDE
- Deploy and run it in the Reactor

----

Module Completed
================

`Chapter Index <return.html#m07>`__
