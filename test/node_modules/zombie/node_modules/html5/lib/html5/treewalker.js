var HTML5 = require('../html5');
var events = require('events');
var util = require('util');

function error(msg) {
	return {type: 'SerializeError', data: msg};
}

function empty_tag(node) {
	if(node.hasChildNodes()) return error(_("Void element has children"));
	return {type: 'EmptyTag', name: node.tagName, data: node.attributes, namespace: node.namespace};
}

function start_tag(node) {
	return {type: 'StartTag', name: node.tagName, data: node.attributes, namespace: node.namespace};
}

function end_tag(node) {
	return {type: 'EndTag', name: node.tagName, namespace: node.namespace };
}

function text(data, target) {
	var m;
	if(m = new RegExp("^[" + HTML5.SPACE_CHARACTERS + "]+").exec(data)) {
		target.emit('token', {type: 'SpaceCharacters', data: m[0]});
		data = data.slice(m[0].length, data.length);
		if(data.length == 0) return;
	}
	
	if(m = new RegExp("["+HTML5.SPACE_CHARACTERS + "]+$").exec(data)) {
		target.emit('token', {type: 'Characters', data: data.slice(0, m.length)});
		target.emit('token', {type: 'SpaceCharacters', data: data.slice(m.index, data.length)});
	} else {
		target.emit('token', {type: 'Characters', data: data});
	}
}

function comment(data) {
	return {type: 'Comment', data: data};
}

function doctype(node) {
	return {type: 'Doctype', name: node.nodeName, publicId: node.publicId, systemId: node.systemId, correct: node.correct};
}

function unknown(node) {
	return error(_("unknown node: ")+ JSON.stringify(node));
}

function _(str) {
	return str;
}

HTML5.TreeWalker = function (document, dest) {
	if (dest instanceof Function) this.addListener('token', dest);
	walk(document, this);
};

function walk(node, dest) {
	switch(node.nodeType) {
	case node.DOCUMENT_FRAGMENT_NODE:
	case node.DOCUMENT_NODE:
		for(var child = 0; child < node.childNodes.length; ++child) {
			walk(node.childNodes[child], dest);
		}
		break;
	
	case node.ELEMENT_NODE:
		if(HTML5.VOID_ELEMENTS.indexOf(node.tagName.toLowerCase()) != -1) {
			dest.emit('token', empty_tag(node));
		} else {
			dest.emit('token', start_tag(node));
			for(var child = 0; child < node.childNodes.length; ++child) {
				walk(node.childNodes[child], dest);
			}
			dest.emit('token', end_tag(node));
		}
		break;

	case node.TEXT_NODE:
		text(node.nodeValue, dest);
		break;

	case node.COMMENT_NODE:
		dest.emit('token', comment(node.nodeValue));
		break;

	case node.DOCUMENT_TYPE_NODE:
		dest.emit('token', doctype(node));
		break;

	default:
		dest.emit('token', unknown(node));
	}
}		

util.inherits(HTML5.TreeWalker, events.EventEmitter);
