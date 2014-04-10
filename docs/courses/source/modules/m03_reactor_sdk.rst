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

----

Continuuity Reactor SDK
=======================
.fx: center_title_slide

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
::

	bin/
	conf/
	continuuity-api-2.1.0-javadoc.jar
	continuuity-api-2.1.0-source.jar
	continuuity-api-2.1.0.jar
	data/
	docs/
	examples/
	javadocs/
	lib/
	libexec/
	LICENSES/
	README
	VERSION
	web-app/

----

``bin/``
========
::

	data-client
	meta-client
	reactor.bat
	reactor.sh
	run-example
	stream-client

Utility programs for:

- Reactor starting, stopping, status
- Running examples

----

``conf/``
=========
::

	continuuity-site.xml
	logback.xml

Configuration files for:

- Logging
- Metrics
- Router
- App-Fabric


----

``.jar`` Files
==============
::

	continuuity-api-2.1.0-javadoc.jar
	continuuity-api-2.1.0-source.jar
	continuuity-api-2.1.0.jar

- Javadocs and sources for IDEs
- Continuuity Reactor Application 

----

``data/``
=========

- Used by local Reactor
- Storage of system data 
- Storage of user data

----

``docs/``
=========
::

	REST-API-Reference-v<version>.pdf

- Local copy of REST API
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

``examples/``
=============
::

	checkstyle.xml
	common.sh
	CountAndFilterWords/
	CountCounts/
	CountOddAndEven/
	CountRandom/
	CountTokens/
	HelloWorld/
	PageViewAnalytics/
	pom.xml
	Purchase/
	README.rst
	ResourceSpammer/
	ResponseCodeAnalytics/
	SentimentAnalysis/
	SimpleWriteAndRead/
	Ticker/
	TrafficAnalytics/
	WordCount/


- 15 examples
- README.rst includes descriptions

----

``javadocs/``
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

----

``lib/``
========
::

	activation-1.1.jar
	aopalliance-1.0.jar
	app-fabric-2.1.0-api.jar
	app-fabric-2.1.0-integration.jar
	app-fabric-2.1.0-thrift.jar
	app-fabric-2.1.0.jar
	asm-all-4.0.jar
	async-http-client-1.7.18.jar
	  .
	  .
	  .
	weave-api-1.3.0.jar
	weave-common-1.3.0.jar
	weave-core-1.3.0.jar
	weave-discovery-api-1.3.0.jar
	weave-discovery-core-1.3.0.jar
	weave-yarn-1.3.0.jar
	weave-zookeeper-1.3.0.jar
	xmlenc-0.52.jar
	xz-1.0.jar
	zkclient-0.2.jar
	zookeeper-3.4.5.jar

- Libraries required for Continuuity Reactor
- Included in Java ``CLASSPATH`` when started by Reactor scripts

----

Miscellaneous Items
===================
::

	libexec/
	LICENSES/
	README
	VERSION
	web-app/

- ``libexec/``: used on Windows
- ``webapp/``: Continuuity Reactor Dashboard Application


-----

Using the SDK
=============

- Start up the Reactor
- Explore the Reactor Dashboard
- Run the sample Application
- Try out the examples
- Shut down the Reactor when finished

----

Module Objectives
=================

You have:

- Downloaded the Continuuity Reactor SDK
- Explored the contents of the SDK
- Looked at what to do with the SDK


.. include:: ../_slide-fragments/short_spaced_continuuity_copyright.rst

