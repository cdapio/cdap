..
   Copyright © 2012-2014 Cask Data, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

.. _getting_started_toplevel:

.. index::
   single: ZIP Installation

=========================================
ZIP Installation
=========================================

This section of the document will describe how you can download and install the standalone version of CDAP 
using ZIP archive. 

Recommended Platform
====================
  * Unix Flavor
  * Mac OSx

Prerequisite
============
  * Java 1.6+ 
  * Node.js 0.8.14+

Instructions
============

#. Download CDAP Stanadlone by visiting  http://cask.co/downloads or http://cdap.io. The archive has .zip extension.
#. Unzip it using `unzip` utility::
   
   $ unzip cdap-sdk-<version>.zip

#. Start CDAP::

   $ cd cdap-sdk-<version>
   $ bin/cdap.sh start

#. Open the CDAP console by visiting http://localhost:9999


