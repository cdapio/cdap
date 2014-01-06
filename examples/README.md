
Examples
========

This /examples directory contains example apps for the Continuuity Reactor. 
They are not compiled as part of the master build, and they should only depend 
on the API jars (plus their dependencies). However, they may also be provided 
in their compiled forms as JAR files in this release.

Building
========

Each example comes with Maven pom file. To build, install Maven, and from your
/examples directory prompt, type "mvn package" (without the quotes) and then press
Enter.

List of Example Apps
========================

CountAndFilterWords:
--------------------
- A variation of CountTokens that illustrates that a flowlet's output can
  be consumed by multiple downstream flowlets.
- In addition to counting all tokens, also sends all tokens to a filter that
  drops all tokens that are not upper case
- The upper case tokens are then counted by a separate flowlet

CountCounts:
------------
- A very simple flow that counts counts.
- Reads input stream 'text' and tokenizes it. Instead of counting words, it
  counts the number of inputs with the same number of tokens.

CountOddAndEven:
------------
- Consumes generated random numbers and counts odd and even numbers.

CountRandom:
------------
- Generates random numbers between 0 and 9999
- For each number i, generates i%10000, i%1000, i%100, i%10
- Increments the counter for each number.
 
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

HelloWorld:
-------------------
 - This is a simple HelloWorld example that uses one stream, one dataset, one flow and one procedure.
 - A stream to send names to.
 - A flow with a single flowlet that reads the stream and stores each name in a KeyValueTable.
 - A procedure that reads the name from the KeyValueTable and prints Hello [Name]!

ResourceSpammer:
-------------------
- An example designed to stress test CPU resources.

SimpleWriteAndRead:
-------------------
- A simple example to illustrate how to read and write key/values in a flow.

TwitterScanner:
---------------
- A simple flow with a source flowlet that reads tweets.

WordCount:
-----------
- A wordcount application is a very simple application that counts words 
and also tracks word associations and unique words seen on the stream. It 
demonstrates the power of using datasets and how they can be used to simplify 
storing complex data.

