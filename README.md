Examples
========

This contains example flows for Continuuity. These flows are on purpose not
compiled as part of the master build, and they should only depend on the api
jars (plus their dependencies). These serve as sample code for developers,
and we should provide directions on how to build a far file from source,
possibly with an idea or eclipse project file.

Building
========

Each example comes with ant build file. To build, you need to tell ant where
the Bigflow API libraries are located (the default is ../.., which only
works when the examples are built from within the distribution). If the
BIGFLOW_HOME environment variable is set, then it is expected to contain
the libraries. Otherwise, specify the libraries with ant -Dbigflow.lib=...

List of Example Projects
========================

CountTokens:
------------
- Reads events (= byte[] body, Map<String,String> headers) from its input
  stream 'input' (note that is not the default name, which would be 'in').
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
 
