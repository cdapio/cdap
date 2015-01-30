CDAP Angular UI
===============

_It does not work yet._

### To install dependencies:

Global dependencies:

* `npm install -g bower gulp`

Local dependencies:

* `npm install && bower install`


### To get a running backend:

UI work generally requires having a running CDAP-standalone instance. To build an instance: 

* `git clone git@github.com:caskdata/cdap.git`
* `cd cdap`
* `mvn package -pl cdap-standalone -am -DskipTests -P dist,release`
* `cd cdap-standalone/target && unzip cdap-sdk-2.8.0-SNAPSHOT.zip`

If CDAP is located as expected (`cdap/` and `cdap-ui/` are siblings), compiled and unzipped in place, you can start the backend from the `cdap-ui/` directory with:

* `npm run backend start`

### To just run the UI code:

* `gulp build && npm start`

### To work on UI code:

Each in their own tab:

* `gulp watch` (autobuild + livereload)
* `npm start` (http-server)
* `npm test` (run karma for unit tests)
* `open http://localhost:8080`

### To build a release:

* `bin/build.sh` will put necessary items in the `target/` directory.
