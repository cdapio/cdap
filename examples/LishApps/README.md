---
Payvment Lish Feeds on the Continuuity AppFabric
---

This project contains all of the code, build files, dependencies,
documentation, and command-line tools for the implementation of processing
Lish activity and generating activity and popular feeds.

DIRECTORY LAYOUT

README   What you are reading now
bin/	 Command-line clients for sending clusters/actions and reading feeds
distro/  Zip distribution of the Continuuity Developer Suite
doc/     Javadoc for the app
lib/     Continuuity Libraries and other dependencies
pom.xml  Maven project file (see below for some pointers)
src/     The source code for the entire project and tests


GETTING STARTED

To use the command-line tools, just set the environment variable
CONTINUUITY_HOME to point to the root directory of your Continuuity Developer
Suite.

	export CONTINUUITY_HOME=~/continuuity-developer-suite/


BUILDING PROJECT JARS

Currently jars of the flows and queries must include all dependencies and a
jar can only specify a single flow or query at one time.  In the future,
these limitations should go away and only the gson/opencsv/guava jars will
be needed as they are the only ones used outside of tests.

In maven, profiles have been setup for each of the three types.

To generate the uber jar for one flow, do the following:

	mvn compile assembly:single -Pcluster-writer

To generate all jars:

	mvn compile
	mvn assembly:single -Pcluster-writer
	mvn assembly:single -Psocial-action
	mvn assembly:single -Pfeed-reader

This will generate three jars in the target/ directory.
