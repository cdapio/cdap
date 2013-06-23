#!/usr/bin/env node
var fs = require('fs');

var doc = fs.readFileSync(process.argv[2]);

var HTML5 = require('../lib/html5');
HTML5.enableDebug('tokenizer');
HTML5.enableDebug('parser');

var p = new HTML5.Parser();
p.parse(doc);

require('util').puts(HTML5.serialize(p.tree.document));
