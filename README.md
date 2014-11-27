CDAP Angular UI
===============

_It does not work yet._

### Installation:

Global dependencies:

* `npm install -g bower gulp`

Local dependencies:

* `npm install && bower install`

UI work generally requires having a running CDAP-standalone instance. If CDAP is located as expected, compiled and unzipped in place, you can start it with:

* `npm run backend start`

Then, each in their own tab:

* `gulp watch` (autobuild + livereload)
* `npm start` (http-server)
* `npm test` (run karma for unit tests)
* `open http://localhost:8080`

### To build a release:

* `gulp distribute` (minify and rev-tag assets)
