======================================
Cask Data Application Platform Example
======================================

This ``example`` directory contains an example app for the Cask Data Application Platform
(CDAP). Detailed information about this example is available at the Cask documentation website:

  http://docs.cask.co/cdap/current/en/examples-manual/examples/index.html

A complete list of example applications is available in the parent directory.


Building
========

The example comes with a Maven pom.xml file. To build, install Maven, and from the
directory prompt, enter::

  mvn clean package


Summary of Application
======================

WordCount
---------
- A simple application that counts words and tracks word associations and unique words
  seen on the Stream. It demonstrates the power of using Datasets and how they can be used
  to simplify storing complex data.


Cask is a trademark of Cask Data, Inc. All rights reserved.

Copyright © 2014-2015 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
except in compliance with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
either express or implied. See the License for the specific language governing permissions
and limitations under the License.
