var HTML5 = require('../html5');
var events = require('events');

function keys(o) {
	var r = [];
	for(var k in o) {
		r.push(k);
	}
	return r;
}

function hescape(s) {
      return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
}

var default_opts = {
	lowercase: true,
	minimize_boolean_attributes: true,
	quote_attr_values: true,
	use_best_quote_char: true,
	use_trailing_solidus: true,
	escape_lt_in_attrs: true,
	space_before_trailing_solidus: true
}

HTML5.serialize = function(src, target, override) {
	var options;
	if(!override) {
		options = default_opts
	} else {
		options = {}
		var k;
		for(k in default_opts) options[k] = default_opts[k]
		for(k in override) options[k] = override[k]
	}
	var dest;
	if(target instanceof Function) {
		dest = new events.EventEmitter();
		dest.addListener('data', target);
	} else if(!target) {
		dest = new events.EventEmitter();
		var ret = '';
		dest.addListener('data', function(d) {
			ret += d;
		});
	} else {
		dest = target;
	}
	var strict = false;
	var errors = [];

	function serialize_error(data) {
		errors.push(data);
		if(strict) throw(data);
	}
		
	var in_cdata = false;
	//TODO: Filters
	var doctype;
	var escape_rcdata = false;
	var w = new HTML5.TreeWalker(src, function(tok) {
		if(tok.type == "Doctype") {
			doctype = "<!DOCTYPE " + tok.name + ">";
			dest.emit('data', doctype);
		} else if(tok.type == 'Characters' || tok.type == 'SpaceCharacters') {
			if(in_cdata || tok.type == 'SpaceCharacters') {
				if(in_cdata && tok.data.indexOf("</") != -1) {
					serialize_error("Unexpected </ in CDATA")
				}
				dest.emit('data', tok.data);
			} else {
				if(tok.data) dest.emit('data', hescape(tok.data));
			}
		} else if(tok.type == "StartTag" || tok.type == 'EmptyTag') {
			if(HTML5.RCDATA_ELEMENTS.indexOf(tok.name.toLowerCase()) != -1 && !escape_rcdata) {
				in_cdata = true;
			} else if (in_cdata) {
				serialize_error("Unexpected child element of a CDATA element");
			}
			var attributes = "";
			var attrs= [];
			for(var ki = 0; ki < tok.data.length; ki++) {
				attrs.push(tok.data.item(ki));
			}
			attrs = attrs.sort();
			for(var ki in attrs) {
				var quote_attr = false;
				var v = tok.data.getNamedItem(attrs[ki].nodeName).nodeValue;
				attributes += " "+attrs[ki].nodeName;
				if(!options.minimize_boolean_attributes || ((HTML5.BOOLEAN_ATTRIBUTES[tok.name] || []).indexOf(ki) == -1 && (HTML5.BOOLEAN_ATTRIBUTES["_global"].indexOf(ki) == -1))) {
					attributes += "=";
					if(options.quote_attr_values || v.length == 0) {
						quote_attr = true;
					} else {
						quote_attr = new RegExp("[" + HTML5.SPACE_CHARACTERS_IN + "<=>'\"" + "]").test(v)
					}

					v = v.replace(/&/g, '&amp;');
					if(options.escape_lt_in_attrs) v = v.replace(/</g, '&lt;');
					if(quote_attr) {
						var the_quote_char = '"';
						if(options.use_best_quote_char) {
							if(v.indexOf("'") != -1 && v.indexOf('"') == -1) {
								the_quote_char = '"';
							} else if(v.indexOf('"') != -1 && v.indexOf("'") == -1) {
								the_quote_char = "'"
							}
						}
						if(the_quote_char == '"') {
							v = v.replace(/"/g, '&quot;');
						} else {
							v = v.replace(/'/g, '&#39;');
						}
						attributes += the_quote_char + v + the_quote_char;
					} else {
						attributes += v;
					}
				}
			}

			if(HTML5.VOID_ELEMENTS.indexOf(tok.name.toLowerCase()) != -1 && options.use_trailing_solidus) {
				if(options.space_before_trailing_solidus) {
					attributes += " /";
				} else {
					attributes += "/";
				}
			}

			if(options.lowercase) tok.name = tok.name.toLowerCase()

			dest.emit('data', "<" + tok.name + attributes + ">");

		} else if(tok.type == 'EndTag') {
			if(HTML5.RCDATA_ELEMENTS.indexOf(tok.name.toLowerCase()) != -1) {
				in_cdata = false;
			} else if(in_cdata) {
				serialize_error("Unexpected child element of a CDATA element");
			}
	
			if(options.lowercase) tok.name = tok.name.toLowerCase()
			dest.emit('data', '</' + tok.name + '>');
		} else if(tok.type == 'Comment') {
			if(tok.data.match(/--/)) serialize_error("Comment contains --");
			dest.emit('data', '<!--' + tok.data + '-->');
		} else {
			serialize_error(tok.data);
		}
	});

	dest.emit('end')

	if(ret) return ret;
}
