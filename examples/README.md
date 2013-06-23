
Examples - 2
========

This contains example flows for Continuuity. These flows are on purpose not
compiled as part of the master build, and they should only depend on the api
jars (plus their dependencies). These serve as sample code for developers,
and we should provide directions on how to build a far file from source,
possibly with an idea or eclipse project file.

Building
========

Each example comes with ant build file. To build, you need to tell ant where
the AppFabric API libraries are located (the default is ../.., which only
works when the examples are built from within the distribution). If the
APP_FABRIC_HOME environment variable is set, then it is expected to contain
the libraries. Otherwise, specify the libraries with ant -Dappfabric.lib=...

If you want to override the naming convention for your Main class that is
identified in the MANIFEST.MF file in your Flow's jar, you can do this
in your local ant build file with the following syntax:

<?xml version="1.0" ?>
<project name="TwitterScanner" default="jar">

    <!-- Override the default main class defn -->
    <property name="main.class" value="TwitterScanner.TwitterFlow"/>

    <!-- Now import the common build file -->
    <import file="../ant-common.xml"/>

</project>

Note: Due to the way Ant handles properties, it is important to insert the
property BEFORE you import the common build file.

Some of the examples include maven configuration pom and can be built with
maven build tool as well. CountCounts is an example with third-party
dependencies (jackson JSON lib) and requires maven for building ('mvn'
command should be available on command line).

List of Example Projects
========================

CountTokens:
------------
- Reads events (= byte[] body, Map<String,String> headers) from input
  stream 'text'.
- Tokenizes the text in the body and in the header named 'title', ignores
  all other headers.
- Each token is cloned into two tokens:
  a) the upper cased version of the token
  b) the original token with a field prefix ('title', or if the token is from
     the body of the event, 'text')
- All of the cloned tokens are counted using increment operations.

CountRandom:
------------
- Generates Random numbers between 0 and 9999
- For each number i, spits out i%10000, i%1000, i%100, i%10
- For each number increment its counter.
 
CountAndFilterWords:
--------------------
- A variation of CountTokens that illustrates that a flowlet's output can
  be consumed by multiple downstream flowlets.
- In addition to counting all tokens, also sends all tokens to a filter that
  drops all tokens that are not upper case
- The upper case tokens are then counted by a separately flowlet

CountCounts:
------------
- A very simple flow that counts counts.
- Reads input stream 'text' and tokenizes it. Instead of counting words, it
  counts the number of inputs with the same number of tokens.

SimpleWriteAndRead:
-------------------
- A simple example to illustrate how to read and write key/values in a flow.

TwitterScanner:
---------------
- A simple flow with a source flowlet that reads tweets.

WordCount:
-----------
- A wordcount application is a very simple application that does the word counting
and also tracks the word associations and unique words seen on the stream. It 
demonstrates the power of using Datasets and how they can be used to simplify storing more
complex data.

DependencyRandomNumber:
-----------------------
- This flow is built with Maven rather than Ant and includes an external
  dependency.
