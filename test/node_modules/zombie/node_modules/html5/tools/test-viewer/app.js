
/**
 * Module dependencies.
 */

var express = require('express');

var app = module.exports = express.createServer();

// Configuration

app.configure(function(){
  app.set('views', __dirname + '/views');
  app.set('view engine', 'jade');
  app.use(express.bodyDecoder());
  app.use(express.methodOverride());
  app.use(app.router);
  app.use(express.staticProvider(__dirname + '/public'));
});

app.configure('development', function(){
  app.use(express.errorHandler({ dumpExceptions: true, showStack: true })); 
});

app.configure('production', function(){
  app.use(express.errorHandler()); 
});

var toHTML = function(str) {
	return str.replace(/&/gm, "&amp;")
		.replace(/</gm, "&lt;")
		.replace(/>/gm, "&gt;")
		.replace(/ /gm, "&nbsp;")
		.replace(/\t/gm, "&nbsp;&nbsp;&nbsp;")
		.replace(/\n/gm, "<br>")
	return str
}

// Routes

var fs = require('fs');
var base = '../../testdata/tree-construction/'
var l = fs.readdirSync(base);
var tests = []
var HTML5 = require('../../lib/html5')
var serialize = require('../../tests/support/serializeTestOutput').serializeTestOutput;
for(var t in l) {
	var testname = l[t];
	if(testname.match(/\.js$/)) continue;
        if(fs.statSync(base+testname).isDirectory()) continue;
	var f = require('../../tests/support/readTestData')
	var td = f.readTestData(base+testname);
	for(var i in td) {
		app.get('/'+testname+'-'+i, (function(td) { 
			tests.push(testname + '-' + i)
			return function(req, res) {
				HTML5.debug('testdata.data', "Data: " + td.data);
				HTML5.debug('testdata.data', "Fragment: " + td['document-fragment']);
				var p = new HTML5.Parser()
				if(td['document-fragment']) {
					p.parse_fragment(td.data.slice(0, td.data.length - 1), td['document-fragment'].trimRight())
				} else {
					p.parse(td.data.slice(0, td.data.length - 1));
				}
				var serialized = serialize(p.inner_html ? p.tree.getFragment() : p.tree.document);
				res.render('output', {
					locals: {
						p: p,
						serialized: serialized,
						td: td,
						title: 'Output',
						toHTML: toHTML
					}
				})
				return
			 }
		})(td[i]))
	}
}

app.get('/', function(req, res){
  res.render('index', {
    locals: {
      title: 'Tests',
      tests: tests
    }
  });
});

// Only listen on $ node app.js

if (!module.parent) {
  app.listen(3000);
  console.log("Express server listening on port %d", app.address().port)
}
