=======================
Continuuity Reactor SDK
=======================

.. include:: ../_slide-fragments/spaced_continuuity_copyright.rst

----

Module Objectives
=================

In this module, you will look at:

- Downloading the Continuuity Reactor SDK
- Exploring the contents of the SDK
- What do you do with the SDK?

.. what does it contain?
.. what is and what does it contain
.. list of contents
.. javadoocs
.. rest API hguide
.. examples
.. scripts
.. how to deploy
.. how to start
.. how to find answers to questions

----

Using the SDK
=============

- Start up the Reactor
- Explore the Reactor Dashboard
- Run the sample Application
- Try out the examples
- Shut down the Reactor when finished

----

Download the Software Development Kit
=====================================

- ``http://www.continuuity.com/download``
- Register for free account
- Download ``continuuity-sdk-<version>.zip``
- Unzip the download

----

Contents of SDK
===============

- Continuuity Reactor Application
- Continuuity Reactor Dashboard Web Application
- Continuuity Reactor API Code
- Scripts for starting, stopping, getting status of Reactor
- Examples
- Documentation

  - Javadocs
  - REST API Guide
  - Online documentation

----

Continuuity Reactor Application
===============================

- Started by scripts in ``bin/``
- Viewed by running *Dashboard*
- Uses libraries in ``lib/``

----

Continuuity Reactor Dashboard
=============================

- Started by scripts in ``bin/``
- Viewed by connecting to ``http://localhost:9999``

.. image:: ../../../developer-guide/source/_images/dashboard/dashboard_01_overview.png
   :width: 600px

----

Continuuity Reactor API Code
============================

- Three .jar files:
  ::

	continuuity-api-2.1.0-javadoc.jar
	continuuity-api-2.1.0-source.jar
	continuuity-api-2.1.0.jar

----

Scripts
=======

- Utility programs for:

  - Reactor starting, stopping, status

    - ``reactor.sh`` for \*nix systems
    - ``reactor.bat`` for Windows

  - Running examples

- Located in ``bin/``

  ::

	data-client
	meta-client
	reactor.bat
	reactor.sh
	run-example
	stream-client


----

Examples
=============
::

	CountAndFilterWords/
	CountCounts/
	CountOddAndEven/
	CountRandom/
	CountTokens/
	HelloWorld/
	PageViewAnalytics/
	Purchase/
	ResourceSpammer/
	ResponseCodeAnalytics/
	SentimentAnalysis/
	SimpleWriteAndRead/
	Ticker/
	TrafficAnalytics/
	WordCount/

- 15 examples
- ``README.rst`` includes descriptions
- located in ``examples/``:

----

Javadocs
=============
::

	allclasses-frame.html
	allclasses-noframe.html
	com/
	constant-values.html
	deprecated-list.html
	help-doc.html
	index-all.html
	index.html
	overview-frame.html
	overview-summary.html
	overview-tree.html
	package-list
	resources/
	serialized-form.html
	stylesheet.css

- Javadocs for Java portion of API
- REST API documented separately
- Examples & Documentation are best place to start to learn
- Located in ``javadocs/``

----

REST API Guide
==============

- Local copy of REST API located in ``docs/``::

	REST-API-Reference-v<version>.pdf

- All other docs online
- ``http://www.continuuity.com/developers/``

----

Online Documentation
====================

- Introduction
- Examples
- Quickstart
- Programming Guide
- Operations Guide
- Advanced Features
- Javadocs
- REST API
- FAQ

At ``http://www.continuuity.com/developers/``

----

Miscellaneous Items
===================
::

	conf/
	data/
	lib/
	libexec/
	LICENSES/
	README
	VERSION
	web-app/

- ``conf/``: configuration files for logging, metrics, router
- ``data/``: used by local Reactor
- ``lib/``: libraries required for Continuuity Reactor
- ``libexec/``: used on Windows
- ``webapp/``: Continuuity Reactor Dashboard Application


-----

Module Summary
==============

You have:

- Downloaded the Continuuity Reactor SDK
- Explored the contents of the SDK
- Looked at what to do with the SDK


