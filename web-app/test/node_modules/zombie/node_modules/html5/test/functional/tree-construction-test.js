#!/usr/bin/env node
"use strict";
var HTML5 = require('../../lib/html5'),
	events = require('events'),
	fs = require('fs'),
	test = require('tap').test,
	serialize = require('../lib/serializeTestOutput').serializeTestOutput;

var base = __dirname + '/../../data/tree-construction/'
var testList = fs.readdirSync(base);

if (typeof process.argv[2] != 'undefined') {
    var debugs = process.argv[2].split(',')
    for (var i in debugs) {
        HTML5.enableDebug(debugs[i])
    }
}

function doTest(testName) {
	test(testName, function (t) {
		var todo = false;
		var testData = {};
		try {
			testData = JSON.parse(fs.readFileSync(testName+'/info.json'));
		} catch(e) {
			if (e.code !== 'ENOENT') throw e;
		}

		try {
			todo = JSON.parse(fs.readFileSync(testName + '/todo.json'));
		} catch (e) {
			if (e.code !== 'ENOENT') throw e;
		}

		var input = fs.readFileSync(testName+'/input.html', 'utf-8');
		var document = fs.readFileSync(testName+'/result.tree', 'utf-8');

		try {
			var p = new HTML5.Parser()
			if (testData['document-fragment']) {
				p.parse_fragment(input.slice(0, input.length - 1), testData['document-fragment'].trimRight())
			} else {
				p.parse(input.slice(0, input.length - 1));
			}
			var serialized = serialize(p.inner_html ? p.tree.getFragment() : p.tree.document);
			t.equal(serialized, document, "Document '"+testName+"' matches example data" + (todo ? " #TODO There are still edge cases that need help!" : ""))
		} catch (e) {
			t.fail(e.message + " in document '" + testName + "'" + (todo ? " #TODO There are still edge cases that need help!" : ""))
		}
		t.end()
	})
}

for (var i in testList) {
	var testname = testList[i];

	if (fs.statSync(base+testname).isDirectory() && fs.statSync(base+testname+'/input.html')) {
		doTest(base+testname);
	}
}
