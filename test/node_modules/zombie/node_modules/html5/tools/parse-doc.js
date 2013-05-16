#!/usr/bin/env node
var HTML5 = require('../lib/html5'),
	events = require('events'),
	fs = require('fs'),
	assert = require('assert')

var inp = fs.createReadStream(process.argv[2])

var p = new HTML5.Parser();

if(process.argv[3]) {
	var debugs = process.argv[3].split(',');
	for(var d in debugs) HTML5.enableDebug(debugs[d]);
}

p.on('end', function() {
	console.log(require('../test/lib/serializeTestOutput').serializeTestOutput(p.document))
})

p.parse(inp);

