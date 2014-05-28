===================================================
Building Your First Continuuity Reactor Application
===================================================

.. reST Editor: .. section-numbering::
.. reST Editor: .. contents::

.. Slide Presentation HTML Generation
.. landslide: theme ../_theme/slides-generation/
.. landslide: build ../../html/

.. include:: ../_slide-fragments/continuuity_logo.rst

.. |br| raw:: html

   <br />

.. |br2| raw:: html

   <br /><br />

----

Exercise Objectives
====================

In this exercise, you will:

- Use the Continuuity Reactor ``maven`` archetype
- Build a Continuuity Reactor Application
- Deploy and run the application

----

Exercise Steps: Create the Project
==================================

- In a command-line window, create a new, empty directory
- Run the maven archetype command:

.. sourcecode:: shell-session

    $ mvn archetype:generate \
      -DarchetypeCatalog=http://tinyurl.com/ndoa5l2 \
      -DarchetypeGroupId=com.continuuity \
      -DarchetypeArtifactId=reactor-app-archetype \
      -DarchetypeVersion=2.1.0

- For *groupId* use ``com.example``
- For *artifactId* (the project name) use ``BigDataApp``

----

Exercise Steps: Build and Deploy
================================

- Build the resulting project

.. sourcecode:: shell-session

	$ cd BigDataApp
	$ mvn clean package
	
- Deploy the resulting application to a Reactor
- Open the maven project in IntelliJ [Extra credit!] 

----

Exercise Summary
===================

You should now be able to:

- Use the Continuuity Reactor ``maven`` archetype
- Create a simple Continuuity Reactor Application
- Deploy and run it in the Reactor
- Open it in an IDE

----

Exercise Completed
==================

`Chapter Index <return.html#e03>`__

