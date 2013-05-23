var HTML5 = require('../html5');
var util = require('util');

var debugFlags = {any: true}

HTML5.debug = function(section) {
	if(debugFlags[section] || debugFlags[section.split('.')[0]]) {
		var out = [];
		for(var i in arguments) {
			out.push(arguments[i])
		}
		console.log(util.inspect(out, false, 3))
	}
}

HTML5.enableDebug = function(section) {
	debugFlags[section] = true;
}

HTML5.disableDebug = function(section) {
	debugFlags[section] = false;
}

HTML5.dumpTagStack = function(tags) {
	var r = [];
	for(var i in tags) {
		r.push(tags[i].tagName);
	}
	return r.join(', ');
}
