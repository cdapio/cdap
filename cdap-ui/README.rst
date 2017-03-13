=======
CDAP UI
=======

CDAP UI comprises three different webapps -- CDAP, Hydrator, and Tracker -- that we ship as part of every release.
CDAP is built in React while Hydrator and Tracker are written in Angular.

Building the UI
===============

Prerequisites
-------------
- NodeJS Version: 4.5.0 or higher

  CDAP UI requires a minimum NodeJS version of 4.5.0.
  You could either download from the nodejs.org website or use a version manager.

  - `v4.5.0 Download <https://nodejs.org/download/release/v4.5.0/>`__

  - `nvm <https://github.com/creationix/nvm#install-script>`__ or

  - `n <https://github.com/tj/n>`__ from github.

  The node version managers help switching between node version quite seamlessly.

- Build tools: ``gulp``, ``webpack``, and ``bower``

  CDAP UI extensively uses ``bower``, ``gulp``, and ``webpack`` during its build process.
  Even though it's not necessary, it will be useful if they are installed globally::

    $ npm install gulp bower webpack -g

Install Dependencies
--------------------
::

  $ npm install
  $ bower install


Building CDAP in React
======================
::

  $ npm run cdap-dev-build ## build version
  $ npm run cdap-dev-build-w ## watch version


Building Hydrator and Tracker in Angular
========================================
::

  $ npm run build ## build version
  $ npm run build-w ## watch version


Building DLLs for updating pre-built libraries used by CDAP
===========================================================
::

  $ npm run build-dlls

This will build the pre-built library dlls that we use in CDAP


Building a Running Backend
==========================
UI work generally requires having a running Standalone CDAP instance. To build an instance::

    $ git clone git@github.com:caskdata/cdap.git
    $ cd cdap
    $ mvn package -pl cdap-standalone -am -DskipTests -P dist,release
    $ cd cdap-standalone/target
    $ unzip cdap-sdk-{version}.zip
    $ cd <cdap-sdk-folder>
    $ bin/cdap sdk start

Once you have started the Standalone CDAP, it starts the UI node server as part of its init script.

To work on UI Code
------------------
If you want to develop and test the UI against the Standalone CDAP that was just built as above,
you need to first kill the node server started by the Standalone CDAP and follow this process:

Start these processes, each in their own terminal tab or browser window:

- ``$ gulp watch`` (autobuild + livereload of angular app)
- ``$ npm run cdap-dev-build-w`` (autobuild + livereload of react app)
- ``$ npm start`` (http-server)
- ``$ open http://localhost:11011``

If you are working on common components shared between all the apps (for instance, the Header)
then you need to build an additional ``common`` library that is used across all:

- ``$ webpack --config webpack.config.common.js -d ## build version``
- ``$ webpack --config webpack.config.common.js --watch -d ## watch version``


======================
License and Trademarks
======================

Copyright © 2016-2017 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied. See the License for the specific language governing permissions
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.
