var HTML5 = require('../../lib/html5');
var dom = HTML5.DOM;
var walker = HTML5.TreeWalker;

exports.serializeTestOutput = function(doc) {
	var s = '';
	var indent = '';
	new walker(doc, function(token) {
		switch(token.type) {
		case 'StartTag':
			var n = '';
			if(token.namespace) {
				n = token.namespace + ' ';
			}
			s += indent + '<' + n + token.name.toLowerCase() + ">\n";
			indent += '  ';
			var a = []
			for(var i = 0; i < token.data.length; i++) {
				a.push(token.data.item(i))
			}
			a = a.sort(function(a1, a2) { 
				if( a1.nodeName < a2.nodeName) return -1
				if( a1.nodeName > a2.nodeName) return 1;
				if( a1.nodeName == a2.nodeName) return 0;
			});
			for(var i = 0; i < a.length; i++) {
				s += indent + (a[i].namespace ? a[i].namespace + ' ' : '') + a[i].nodeName + '="' + a[i].nodeValue + '"\n'
			}
			break;
		case 'EmptyTag':
			s += indent + '<' + token.name.toLowerCase() + '>\n';
			var a = []
			for(var i = 0; i < token.data.length; i++) {
				a.push(token.data.item(i))
			}
			a = a.sort(function(a1, a2) { 
				if( a1.nodeName < a2.nodeName) return -1
				if( a1.nodeName > a2.nodeName) return 1;
				if( a1.nodeName == a2.nodeName) return 0;
			});
			for(var i = 0; i < a.length; i++) {
				s += indent + '  ' + (a[i].namespace ? a[i].namespace + ' ' : '') + a[i].nodeName + '="' + a[i].nodeValue + '"\n'
			}
			break;
		case 'EndTag':
			indent = indent.slice(2);
			break;
		case 'Characters':
			s += indent + '"' + token.data + '"\n';
			break;
		case 'Comment':
			s += indent + '<!-- ' + token.data + ' -->\n';
			break;
		case 'Doctype':
			s += indent + '<!DOCTYPE ' + token.name + '>\n';
			break;
		}
	});
	return s;
}
