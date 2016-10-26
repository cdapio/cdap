========
 CDAP UI
========

CDAP UI comprises three different webapps - CDAP, Hydrator & Tracker, that we ship as part of every release. CDAP is built in React while Hydrator and Tracker are written in Angular.

------------
 To build UI
------------

^^^^^^^^^^^^^^
 Prerequisites
^^^^^^^^^^^^^^
- NodeJS Version - 4.5.0

  CDAP UI requires a minimum NodeJS version of 4.5.0. You could either download (from nodejs.org website) or use a version manager.

  - `v4.5.0 Download <https://nodejs.org/download/release/v4.5.0/>`_

  - `nvm <https://github.com/creationix/nvm#install-script>`_ or
  - `n <https://github.com/tj/n>`_ from github.

  The node version managers help switching between node version quite seamlessly.

-  Build tools - ``gulp``, ``webpack`` & ``bower``

  CDAP UI extensively uses `bower`, `gulp` & `webpack` during its build process. Even though its not necessary it will be useful if they are installed globally::

    npm instal gulp bower webpack -g


^^^^^^^^^^^^^^^^^^^^^
 Install dependencies
^^^^^^^^^^^^^^^^^^^^^

::

  npm install
  bower install


^^^^^^^^^^^^^^^^^^^
Build CDAP in React
^^^^^^^^^^^^^^^^^^^

::

  npm run cdap-dev-build ## build version
  npm run cdap-dev-buil-w ## watch version

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
 Build Hydrator & Tracker in Angular
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

::

  npm run build ## build version
  npm run build-w ## watch version


---------------------------
 To build a running backend
---------------------------

UI work generally requires having a running CDAP-standalone instance. To build an instance::

    git clone git@github.com:caskdata/cdap.git
    cd cdap
    mvn package -pl cdap-standalone -am -DskipTests -P dist,release
    cd cdap-standalone/target
    unzip cdap-sdk-{version}.zip
    cd <cdap-sdk-folder>/bin/cdap sdk start


Once you have started the SDK it starts the UI node server as part of its init script.

-------------------
 To work on UI code
-------------------

If you want to develop and test UI against the SDK that was just built above you need to first kill the node server started by the SDK and follow process mentioned below,

Each in their own tab

- ``gulp watch`` (autobuild + livereload of angular app)
- ``npm run cdap-dev-build-w`` (autobuild + livereload of react app)
- ``npm start`` (http-server)
- ``open http://localhost:11011``

If you are working on common components shared between all the apps (for instance the Header) then you need to build an additional `common` library that we use across all as follows,

- ``webpack --config webpack.config.common.js -d ## build version``
- ``webpack --config webpack.config.common.js --watch -d ## watch version``


======================
License and Trademarks
======================

Copyright © 2015 Cask Data, Inc.

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
