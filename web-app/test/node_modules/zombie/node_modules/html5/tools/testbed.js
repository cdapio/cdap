var HTML5 = require('../lib/html5'),
	events = require('events'),
	util = require('util'),
	fs = require('fs'),
	assert = require('assert'),
	serialize = require('../test/lib/serializeTestOutput').serializeTestOutput;

if(process.argv[4]) {
	var debugs = process.argv[4].split(',');
	for(var d in debugs) HTML5.enableDebug(debugs[d]);
}

var base = __dirname + '/../data/tree-construction/'
var l = fs.readdirSync(base);
for(var t in l) {
	if(process.argv[2] && process.argv[2] != l[t]) continue;
	var testname = l[t];
	if(testname.match(/\.js$/)) continue;
        if(fs.statSync(base+testname).isDirectory()) continue;
	util.debug("Test file: " + testname);
	var f = require('../test/lib/readTestData')
	var td = f.readTestData(base+testname);
	var tests = 1;
	for(var i in td) {
		try {
			if(process.argv[3] && process.argv[3] != i) continue;
			util.debug("Test #" + i + ": ");
			util.debug("Input data: " + util.inspect(td[i].data.slice(0, td[i].data.length - 1)));
			if(td[i]['document-fragment']) util.debug("Input document fragment: " + util.inspect(td[i]['document-fragment']))
			var p = new HTML5.Parser()
			if(td[i]['document-fragment']) {
				p.parse_fragment(td[i].data.slice(0, td[i].data.length - 1), td[i]['document-fragment'].slice(0, td[i]['document-fragment'].length - 1))
			} else {
				p.parse(td[i].data.slice(0, td[i].data.length - 1));
			}
			var errorsFixed = p.errors.map(function(e) {
				if(!HTML5.E[e[0]]) return e;
				return HTML5.E[e[0]].replace(/%\(.*?\)/, function(r) {
					if(e[1]) {
						return e[1][r.slice(2).slice(0, r.length - 3)];
					} else {
						return r;
					}
				});
			});

			assert.ok(p.document);
			HTML5.debug('testbed', "parse complete");

			HTML5.debug('testdata.errors', "Expected ", td[i].errors);
			HTML5.debug('testdata.errors', "Actual ", errorsFixed);
			var serialized = serialize(p.inner_html ? p.fragment : p.document);
			util.debug("Output : " + serialized);
			//util.debug("Tree : " + require('util').inspect(p));
			util.debug("Check  : " + td[i].document);
			assert.deepEqual(serialized, td[i].document);
			if(td[i].errors && p.errors.length !== td[i].errors.length) {
				util.debug("Expected errors: " + util.inspect(td[i].errors));
				util.debug("Actual errors  : " + util.inspect(p.errors));
			}
		} catch(e) {
                        util.debug('error in parsing: ' + e.message + " " + e.stack);
		}
	}
}
