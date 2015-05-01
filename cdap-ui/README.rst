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
