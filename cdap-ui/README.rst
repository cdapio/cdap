===============
CDAP Angular UI
===============


To install dependencies:
========================

Global dependencies::

  npm install -g bower gulp

Local dependencies::

  npm install && bower install


To build a running backend
==========================

UI work generally requires having a running CDAP-standalone instance. To build an instance::

  git clone git@github.com:caskdata/cdap.git
  cd cdap
  mvn package -pl cdap-standalone -am -DskipTests -P dist,release
  cd cdap-standalone/target && unzip cdap-sdk-{version}.zip

You can start the standalone backend from the ``cdap-ui/`` directory with::

  npm run backend start

To just run the UI code::

  gulp build && npm start


To work on UI code
==================

Each in their own tab:

- ``gulp watch`` (autobuild + livereload)
- ``npm start`` (http-server)
- ``npm test`` (runs karma for unit tests)
- ``open http://localhost:8080``


To build a release
==================

This will put necessary items in the ``target/`` directory::

  bin/build.sh


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
