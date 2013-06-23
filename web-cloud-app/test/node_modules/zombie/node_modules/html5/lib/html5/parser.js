var HTML5 = exports.HTML5 = require('../html5');

var assert = require('assert');
var events = require('events');
var util = require('util');

require('./treebuilder');
require('./tokenizer');

var Parser = HTML5.Parser = function HTML5Parser(options) {
	var parser = this;
	events.EventEmitter.apply(this);
	this.strict = false;
	this.errors = [];
	var phase;
	var phases = {};
	var secondary_phase;

	this.__defineSetter__('phase', function(p) {
		phase = p;
		if (!p) throw( new Error("Can't leave phase undefined"));
		if (!p instanceof Function) throw( new Error("Not a function"));
	});

	this.__defineGetter__('phase', function() {
		return phase;
	});

	this.newPhase = function(name) {
		this.phase = phases[name];
		HTML5.debug('parser.newPhase', name);
		this.phaseName = name;
	};

	phases.base = {
		end_tag_handlers: {"-default": 'endTagOther'},
		start_tag_handlers: {"-default": 'startTagOther'},
		parse_error: function(code, options) {
			parser.parse_error(code, options);
		},
		processEOF: function() {
			tree.generateImpliedEndTags();
			if (tree.open_elements.length > 2) {
				parser.parse_error('expected-closing-tag-but-got-eof');
			} else if (tree.open_elements.length == 2 &&
				tree.open_elements[1].tagName.toLowerCase() != 'body') {
				// This happens for framesets or something?
				parser.parse_error('expected-closing-tag-but-got-eof');
			} else if (parser.inner_html && tree.open_elements.length > 1) {
				// XXX This is not what the specification says. Not sure what to do here.
				parser.parse_error('eof-in-innerhtml');
			}
		},
		processComment: function(data) {
			// For most phases the following is correct. Where it's not it will be
			// overridden.
			tree.insert_comment(data, tree.open_elements.last());
		},
		processDoctype: function(name, publicId, systemId, correct) {
			parser.parse_error('unexpected-doctype');
		},
		processSpaceCharacters: function(data) {
			tree.insert_text(data);
		},
		processStartTag: function(name, attributes, self_closing) {
			if (this[this.start_tag_handlers[name]]) {
				this[this.start_tag_handlers[name]](name, attributes, self_closing);
			} else if (this[this.start_tag_handlers["-default"]]) {
				this[this.start_tag_handlers["-default"]](name, attributes, self_closing);
			} else {
				throw(new Error("No handler found for "+name));
			}
		},
		processEndTag: function(name) {
			if (this[this.end_tag_handlers[name]]) {
				this[this.end_tag_handlers[name]](name);
			} else if (this[this.end_tag_handlers["-default"]]) {
				this[this.end_tag_handlers["-default"]](name);
			} else {
				throw(new Error("No handler found for "+name));
			}
		},
		inScope: function(name, scopingElements) {
			if (!scopingElements) scopingElements = HTML5.SCOPING_ELEMENTS;
			if (!tree.open_elements.length) return false;
			for(var i = tree.open_elements.length - 1; i >= 0; i--) {
				if (!tree.open_elements[i].tagName) return false;
				if (tree.open_elements[i].tagName.toLowerCase() == name) return true;
				if (scopingElements.indexOf(tree.open_elements[i].tagName.toLowerCase()) != -1) return false;
			}
			return false;
		},
		startTagHtml: function(name, attributes) {
			if (!parser.first_start_tag && name == 'html') {
				parser.parse_error('non-html-root');
			}
			// XXX Need a check here to see if the first start tag token emitted is this token. . . if it's not, invoke parse_error.
			for(var i = 0; i < attributes.length; i++) {
				if (!tree.open_elements[0].getAttribute(attributes[i].nodeName)) {
					tree.open_elements[0].setAttribute(attributes[i].nodeName, attributes[i].nodeValue);
				}
			}
			parser.first_start_tag = false;
		},
		adjust_mathml_attributes: function(attributes) {
			return attributes.map(function(a) {
				if (a[0] =='definitionurl') {
					return ['definitionURL', a[1]];
				} else {
					return a;
				}
			});
		},
		adjust_svg_attributes: function(attributes) {
			return attributes.map(function(a) {
				return HTML5.SVGAttributeMap[a] ? HTML5.SVGAttributeMap[a] : a;
			});
		},
		adjust_foreign_attributes: function (attributes) {
			for(var i = 0; i < attributes.length; i++) {
				if (attributes[i].nodeName.indexOf(':') != -1) {
					var t = attributes[i].nodeName.split(/:/);
					attributes[i].namespace = t[0];
					attributes[i].nodeName = t[1];
				}
			}
			return attributes;
		}
	};

	phases.initial = Object.create(phases.base);

	phases.initial.processEOF = function() {
		parser.parse_error("expected-doctype-but-got-eof");
		parser.newPhase('beforeHTML');
		phase.processEOF();
	};

	phases.initial.processComment = function(data) {
		tree.insert_comment(data, tree.document);
	};

	phases.initial.processDoctype = function(name, publicId, systemId, correct) {
		if (name.toLowerCase() != 'html' || publicId || systemId) {
			parser.parse_error("unknown-doctype");
		}

		// XXX need to update DOCTYPE tokens
		tree.insert_doctype(name, publicId, systemId);

		publicId = (publicId || '').toString().toUpperCase();

		if (name.toLowerCase() != 'html') {
			// XXX quirks mode
		} else {
			if ((["+//silmaril//dtd html pro v0r11 19970101//en",
				"-//advasoft ltd//dtd html 3.0 aswedit + extensions//en",
				"-//as//dtd html 3.0 aswedit + extensions//en",
				"-//ietf//dtd html 2.0 level 1//en",
				"-//ietf//dtd html 2.0 level 2//en",
				"-//ietf//dtd html 2.0 strict level 1//en",
				"-//ietf//dtd html 2.0 strict level 2//en",
				"-//ietf//dtd html 2.0 strict//en",
				"-//ietf//dtd html 2.0//en",
				"-//ietf//dtd html 2.1e//en",
				"-//ietf//dtd html 3.0//en",
				"-//ietf//dtd html 3.0//en//",
				"-//ietf//dtd html 3.2 final//en",
				"-//ietf//dtd html 3.2//en",
				"-//ietf//dtd html 3//en",
				"-//ietf//dtd html level 0//en",
				"-//ietf//dtd html level 0//en//2.0",
				"-//ietf//dtd html level 1//en",
				"-//ietf//dtd html level 1//en//2.0",
				"-//ietf//dtd html level 2//en",
				"-//ietf//dtd html level 2//en//2.0",
				"-//ietf//dtd html level 3//en",
				"-//ietf//dtd html level 3//en//3.0",
				"-//ietf//dtd html strict level 0//en",
				"-//ietf//dtd html strict level 0//en//2.0",
				"-//ietf//dtd html strict level 1//en",
				"-//ietf//dtd html strict level 1//en//2.0",
				"-//ietf//dtd html strict level 2//en",
				"-//ietf//dtd html strict level 2//en//2.0",
				"-//ietf//dtd html strict level 3//en",
				"-//ietf//dtd html strict level 3//en//3.0",
				"-//ietf//dtd html strict//en",
				"-//ietf//dtd html strict//en//2.0",
				"-//ietf//dtd html strict//en//3.0",
				"-//ietf//dtd html//en",
				"-//ietf//dtd html//en//2.0",
				"-//ietf//dtd html//en//3.0",
				"-//metrius//dtd metrius presentational//en",
				"-//microsoft//dtd internet explorer 2.0 html strict//en",
				"-//microsoft//dtd internet explorer 2.0 html//en",
				"-//microsoft//dtd internet explorer 2.0 tables//en",
				"-//microsoft//dtd internet explorer 3.0 html strict//en",
				"-//microsoft//dtd internet explorer 3.0 html//en",
				"-//microsoft//dtd internet explorer 3.0 tables//en",
				"-//netscape comm. corp.//dtd html//en",
				"-//netscape comm. corp.//dtd strict html//en",
				"-//o'reilly and associates//dtd html 2.0//en",
				"-//o'reilly and associates//dtd html extended 1.0//en",
				"-//spyglass//dtd html 2.0 extended//en",
				"-//sq//dtd html 2.0 hotmetal + extensions//en",
				"-//sun microsystems corp.//dtd hotjava html//en",
				"-//sun microsystems corp.//dtd hotjava strict html//en",
				"-//w3c//dtd html 3 1995-03-24//en",
				"-//w3c//dtd html 3.2 draft//en",
				"-//w3c//dtd html 3.2 final//en",
				"-//w3c//dtd html 3.2//en",
				"-//w3c//dtd html 3.2s draft//en",
				"-//w3c//dtd html 4.0 frameset//en",
				"-//w3c//dtd html 4.0 transitional//en",
				"-//w3c//dtd html experimental 19960712//en",
				"-//w3c//dtd html experimental 970421//en",
				"-//w3c//dtd w3 html//en",
				"-//w3o//dtd w3 html 3.0//en",
				"-//w3o//dtd w3 html 3.0//en//",
				"-//w3o//dtd w3 html strict 3.0//en//",
				"-//webtechs//dtd mozilla html 2.0//en",
				"-//webtechs//dtd mozilla html//en",
				"-/w3c/dtd html 4.0 transitional/en",
				"html"].indexOf(publicId) != -1) ||
			(!systemId && ["-//w3c//dtd html 4.01 frameset//EN",
				"-//w3c//dtd html 4.01 transitional//EN"].indexOf(publicId) != -1) ||
			(systemId ==
				"http://www.ibm.com/data/dtd/v11/ibmxhtml1-transitional.dtd")) {
				// XXX quirks mode
			}
		}

		parser.newPhase('beforeHTML');
	};

	phases.initial.processSpaceCharacters = function(data) {
	};

	phases.initial.processCharacters = function(data) {
		parser.parse_error('expected-doctype-but-got-chars');
		parser.newPhase('beforeHTML');
		phase.processCharacters(data);
	};

	phases.initial.processStartTag = function(name, attributes, self_closing) {
		parser.parse_error('expected-doctype-but-got-start-tag', {name: name});
		parser.newPhase('beforeHTML');
		phase.processStartTag(name, attributes);
	};

	phases.initial.processEndTag = function(name) {
		parser.parse_error('expected-doctype-but-got-end-tag', {name: name});
		parser.newPhase('beforeHTML');
		phase.processEndTag(name);
	};

	phases.afterAfterBody = Object.create(phases.base);

	phases.afterAfterBody.start_tag_handlers = {
		html: 'startTagHtml',
		'-default': 'startTagOther'
	};

	phases.afterAfterBody.processComment = function(data) {
		tree.insert_comment(data);
	};

	phases.afterAfterBody.processDoctype = function(data) {
		phases.inBody.processDoctype(data);
	};

	phases.afterAfterBody.processSpaceCharacters = function(data) {
		phases.inBody.processSpaceCharacters(data);
	};

	phases.afterAfterBody.startTagHtml = function(data, attributes) {
		phases.inBody.startTagHtml(data, attributes);
	};

	phases.afterAfterBody.startTagOther = function(name, attributes) {
		parser.parse_error('unexpected-start-tag', {name: name});
		parser.newPhase('inBody');
		phase.processStartTag(name, attributes);
	};

	phases.afterAfterBody.endTagOther = function(name) {
		parser.parse_error('unexpected-end-tag', {name: name});
		parser.newPhase('inBody');
		phase.processEndTag(name);
	};

	phases.afterAfterBody.processCharacters = function(data) {
		parser.parse_error('unexpected-char-after-body');
		parser.newPhase('inBody');
		phase.processCharacters(data);
	};

	phases.afterAfterFrameset = {}; /// FIXME

	phases.afterBody = Object.create(phases.base);
	
	phases.afterBody.end_tag_handlers = {
		html: 'endTagHtml',
		'-default': 'endTagOther'
	};

	phases.afterBody.processComment = function(data) {
		// This is needed because data is to be appended to the html element here
		// and not to whatever is currently open.
		tree.insert_comment(data, tree.open_elements[0]);
	};

	phases.afterBody.processCharacters = function(data) {
		parser.parse_error('unexpected-char-after-body');
		parser.newPhase('inBody');
		phase.processCharacters(data);
	};

	phases.afterBody.processStartTag = function(name, attributes, self_closing) {
		parser.parse_error('unexpected-start-tag-after-body', {name: name});
		parser.newPhase('inBody');
		phase.processStartTag(name, attributes, self_closing);
	};

	phases.afterBody.endTagHtml = function(name) {
		if (parser.inner_html) {
			parser.parse_error('end-html-in-innerhtml');
		} else {
			// XXX This may need to be done, not sure
			// Don't set last_phase to the current phase but to the inBody phase
			// instead. No need for extra parse_errors if there's something after
			// </html>.
			// Try <!doctype html>X</html>X for instance
			parser.last_phase = parser.phase;
			parser.newPhase('afterAfterBody');
		}
	};

	phases.afterBody.endTagOther = function(name) {
		parser.parse_error('unexpected-end-tag-after-body', {name: name});
		parser.newPhase('inBody');
		phase.processEndTag(name);
	};

	phases.afterFrameset = Object.create(phases.base);

	phases.afterFrameset.start_tag_handlers = {
		html: 'startTagHtml',
		noframes: 'startTagNoframes',
		'-default': 'startTagOther'
	};

	phases.afterFrameset.end_tag_handlers = {
		html: 'endTagHtml',
		'-default': 'endTagOther'
	};

	phases.afterFrameset.processCharacters = function(data) {
		parser.parse_error("unexpected-char-after-frameset");
	};

	phases.afterFrameset.startTagNoframes = function(name, attributes) {
		phases.inBody.processStartTag(name, attributes);
	};

	phases.afterFrameset.startTagOther = function(name, attributes) {
		parser.parse_error("unexpected-start-tag-after-frameset", {name: name});
	};

	phases.afterFrameset.endTagHtml = function(name) {
		parser.last_phase = parser.phase;
		parser.newPhase('trailingEnd');
	};

	phases.afterFrameset.endTagOther = function(name) {
		parser.parse_error("unexpected-end-tag-after-frameset", {name: name});
	};

	phases.afterHead = Object.create(phases.base);

	phases.afterHead.start_tag_handlers = {
		html: 'startTagHtml',
		body: 'startTagBody',
		frameset: 'startTagFrameset',
		base: 'startTagFromHead',
		link: 'startTagFromHead',
		meta: 'startTagFromHead',
		script: 'startTagFromHead',
		style: 'startTagFromHead',
		title: 'startTagFromHead',
		"-default": 'startTagOther'
	};

	phases.afterHead.end_tag_handlers = {
		body: 'endTagBodyHtmlBr',
		html: 'endTagBodyHtmlBr',
		br: 'endTagBodyHtmlBr',
		"-default": 'endTagOther'
	};

	phases.afterHead.processEOF = function() {
		this.anything_else();
		phase.processEOF();
	};

	phases.afterHead.processCharacters = function(data) {
		this.anything_else();
		phase.processCharacters(data);
	};

	phases.afterHead.startTagBody = function(name, attributes) {
		tree.insert_element(name, attributes);
		parser.newPhase('inBody');
	};

	phases.afterHead.startTagFrameset = function(name, attributes) {
		tree.insert_element(name, attributes);
		parser.newPhase('inFrameset');
	};

	phases.afterHead.startTagFromHead = function(name, attributes) {
		parser.parse_error("unexpected-start-tag-out-of-my-head", {name: name});
		parser.newPhase('inHead');
		phase.processStartTag(name, attributes);
	};

	phases.afterHead.startTagOther = function(name, attributes) {
		this.anything_else();
		phase.processStartTag(name, attributes);
	};

	phases.afterHead.endTagBodyHtmlBr = function(name) {
		this.anything_else();
		phase.processEndTag(name);
	};

	phases.afterHead.endTagOther = function(name) {
		parser.parse_error('unexpected-end-tag', {name: name});
	};

	phases.afterHead.anything_else = function() {
		tree.insert_element('body', []);
		parser.newPhase('inBody');
	};

	phases.afterHead.processEndTag = function(name) {
		this.anything_else();
		phase.processEndTag(name);
	};

	phases.beforeHead = Object.create(phases.base);

	phases.beforeHead.start_tag_handlers = {
		html: 'startTagHtml',
		head: 'startTagHead',
		'-default': 'startTagOther'
	};

	phases.beforeHead.end_tag_handlers = {
		html: 'endTagImplyHead',
		head: 'endTagImplyHead',
		body: 'endTagImplyHead',
		br: 'endTagImplyHead',
		p: 'endTagImplyHead',
		'-default': 'endTagOther'
	};

	phases.beforeHead.processEOF = function() {
		this.startTagHead('head', {});
		phase.processEOF();
	};

	phases.beforeHead.processCharacters = function(data) {
		this.startTagHead('head', {});
		phase.processCharacters(data);
	};

	phases.beforeHead.processSpaceCharacters = function(data) {
	};

	phases.beforeHead.startTagHead = function(name, attributes) {
		tree.insert_element(name, attributes);
		tree.head_pointer = tree.open_elements.last();
		parser.newPhase('inHead');
	};

	phases.beforeHead.startTagOther = function(name, attributes) {
		this.startTagHead('head', {});
		phase.processStartTag(name, attributes);
	};

	phases.beforeHead.endTagImplyHead = function(name) {
		this.startTagHead('head', {});
		phase.processEndTag(name);
	};

	phases.beforeHead.endTagOther = function(name) {
		parser.parse_error('end-tag-after-implied-root', {name: name});
	};

	phases.beforeHTML = Object.create(phases.base);

	phases.beforeHTML.processEOF = function() {
		this.insert_html_element();
		phase.processEOF();
	};

	phases.beforeHTML.processComment = function(data) {
		tree.insert_comment(data, tree.document);
	};

	phases.beforeHTML.processSpaceCharacters = function(data) {
	};

	phases.beforeHTML.processCharacters = function(data) {
		this.insert_html_element();
		phase.processCharacters(data);
	};

	phases.beforeHTML.processStartTag = function(name, attributes, self_closing) {
		if (name == 'html') parser.first_start_tag = true;
		this.insert_html_element();
		phase.processStartTag(name, attributes);
	};

	phases.beforeHTML.processEndTag = function(name) {
		this.insert_html_element();
		phase.processEndTag(name);
	};

	phases.beforeHTML.insert_html_element = function() {
		var de = tree.document.documentElement;
		if (de) {
			if (de.tagName != 'HTML')
				HTML5.debug('parser.before_html_phase', 'Non-HTML root element!');
			tree.open_elements.push(de);
			while(de.childNodes.length >= 1) de.removeChild(de.firstChild);
		} else {
			var element = tree.createElement('html', []);
			tree.open_elements.push(element);
			tree.document.appendChild(element);
		}
		parser.newPhase('beforeHead');
	};


	phases.inCaption = Object.create(phases.base);

	phases.inCaption.start_tag_handlers = {
		html: 'startTagHtml',
		caption: 'startTagTableElement',
		col: 'startTagTableElement',
		colgroup: 'startTagTableElement',
		tbody: 'startTagTableElement',
		td: 'startTagTableElement',
		tfoot: 'startTagTableElement',
		thead: 'startTagTableElement',
		tr: 'startTagTableElement',
		'-default': 'startTagOther'
	};

	phases.inCaption.end_tag_handlers = {
		caption: 'endTagCaption',
		table: 'endTagTable',
		body: 'endTagIgnore',
		col: 'endTagIgnore',
		colgroup: 'endTagIgnore',
		html: 'endTagIgnore',
		tbody: 'endTagIgnore',
		td: 'endTagIgnore',
		tfood: 'endTagIgnore',
		thead: 'endTagIgnore',
		tr: 'endTagIgnore',
		'-default': 'endTagOther'
	};

	phases.inCaption.ignoreEndTagCaption = function() {
		return !this.inScope('caption', HTML5.TABLE_SCOPING_ELEMENTS);
	};

	phases.inCaption.processCharacters = function(data) {
		phases.inBody.processCharacters(data);
	};

	phases.inCaption.startTagTableElement = function(name, attributes) {
		parser.parse_error('unexpected-end-tag', {name: name});
		var ignoreEndTag = this.ignoreEndTagCaption();
		phase.processEndTag('caption');
		if (!ignoreEndTag) phase.processStartTag(name, attributes);
	};

	phases.inCaption.startTagOther = function(name, attributes) {
		phases.inBody.processStartTag(name, attributes);
	};

	phases.inCaption.endTagCaption = function(name) {
		if (this.ignoreEndTagCaption()) {
			// inner_html case
			assert.ok(parser.inner_html);
			parser.parse_error('unexpected-end-tag', {name: name});
		} else {
			// AT this code is quite similar to endTagTable in inTable
			tree.generateImpliedEndTags();
			if (tree.open_elements.last().tagName.toLowerCase() != 'caption') {
				parser.parse_error('expected-one-end-tag-but-got-another', {
					gotName: "caption",
					expectedName: tree.open_elements.last().tagName.toLowerCase()
				});
			}

			tree.remove_open_elements_until('caption');
		
			tree.clearActiveFormattingElements();

			parser.newPhase('inTable');
		}
	};

	phases.inCaption.endTagTable = function(name) {
		parser.parse_error("unexpected-end-table-in-caption");
		var ignoreEndTag = this.ignoreEndTagCaption();
		phase.processEndTag('caption');
		if (!ignoreEndTag) phase.processEndTag(name);
	};

	phases.inCaption.endTagIgnore = function(name) {
		parser.parse_error('unexpected-end-tag', {name: name});
	};

	phases.inCaption.endTagOther = function(name) {
		phases.inBody.processEndTag(name);
	};


	phases.inCell = Object.create(phases.base);

	phases.inCell.start_tag_handlers = {
		html: 'startTagHtml',
		caption: 'startTagTableOther',
		col: 'startTagTableOther',
		colgroup: 'startTagTableOther',
		tbody: 'startTagTableOther',
		td: 'startTagTableOther',
		tfoot: 'startTagTableOther',
		th: 'startTagTableOther',
		thead: 'startTagTableOther',
		tr: 'startTagTableOther',
		'-default': 'startTagOther'
	};

	phases.inCell.end_tag_handlers = {
		td: 'endTagTableCell',
		th: 'endTagTableCell',
		body: 'endTagIgnore',
		caption: 'endTagIgnore',
		col: 'endTagIgnore',
		colgroup: 'endTagIgnore',
		html: 'endTagIgnore',
		table: 'endTagImply',
		tbody: 'endTagImply',
		tfoot: 'endTagImply',
		thead: 'endTagImply',
		tr: 'endTagImply',
		'-default': 'endTagOther'
	};

	phases.inCell.processCharacters = function(data) {
		phases.inBody.processCharacters(data);
	};

	phases.inCell.startTagTableOther = function(name, attributes) {
		if (this.inScope('td', HTML5.TABLE_SCOPING_ELEMENTS) || this.inScope('th', HTML5.TABLE_SCOPING_ELEMENTS)) {
			this.closeCell();
			phase.processStartTag(name, attributes);
		} else {
			// inner_html case
			parser.parse_error();
		}
	};

	phases.inCell.startTagOther = function(name, attributes) {
		phases.inBody.processStartTag(name, attributes);
	};

	phases.inCell.endTagTableCell = function(name) {
		if (this.inScope(name, HTML5.TABLE_SCOPING_ELEMENTS)) {
			tree.generateImpliedEndTags(name);
			if (tree.open_elements.last().tagName.toLowerCase() != name.toLowerCase()) {
				parser.parse_error('unexpected-cell-end-tag', {name: name});
				tree.remove_open_elements_until(name);
			} else {
				tree.pop_element();
			}
			tree.clearActiveFormattingElements();
			parser.newPhase('inRow');
		} else {
			parser.parse_error('unexpected-end-tag', {name: name});
		}
	};

	phases.inCell.endTagIgnore = function(name) {
		parser.parse_error('unexpected-end-tag', {name: name});
	};

	phases.inCell.endTagImply = function(name) {
		if (this.inScope(name, HTML5.TABLE_SCOPING_ELEMENTS)) {
			this.closeCell();
			phase.processEndTag(name);
		} else {
			// sometimes inner_html case
			parser.parse_error();
		}
	};

	phases.inCell.endTagOther = function(name) {
		phases.inBody.processEndTag(name);
	};

	phases.inCell.closeCell = function() {
		if (this.inScope('td', HTML5.TABLE_SCOPING_ELEMENTS)) {
			this.endTagTableCell('td');
		} else if (this.inScope('th', HTML5.TABLE_SCOPING_ELEMENTS)) {
			this.endTagTableCell('th');
		}
	};


	phases.inColumnGroup = Object.create(phases.base);

	phases.inColumnGroup.start_tag_handlers = {
		html: 'startTagHtml',
		col: 'startTagCol',
		'-default': 'startTagOther'
	};

	phases.inColumnGroup.end_tag_handlers = {
		colgroup: 'endTagColgroup',
		col: 'endTagCol',
		'-default': 'endTagOther'
	};

	phases.inColumnGroup.ignoreEndTagColgroup = function() {
		return tree.open_elements.last().tagName.toLowerCase() == 'html';
	};

	phases.inColumnGroup.processCharacters = function(data) {
		var ignoreEndTag = this.ignoreEndTagColgroup();
		this.endTagColgroup('colgroup');
		if (!ignoreEndTag) phase.processCharacters(data);
	};

	phases.inColumnGroup.startTagCol = function(name, attributes) {
		tree.insert_element(name, attributes);
		tree.pop_element();
	};

	phases.inColumnGroup.startTagOther = function(name, attributes) {
		var ignoreEndTag = this.ignoreEndTagColgroup();
		this.endTagColgroup('colgroup');
		if (!ignoreEndTag) phase.processStartTag(name, attributes);
	};

	phases.inColumnGroup.endTagColgroup = function(name) {
		if (this.ignoreEndTagColgroup()) {
			// inner_html case
			assert.ok(parser.inner_html);
			parser.parse_error();
		} else {
			tree.pop_element();
			parser.newPhase('inTable');
		}
	};

	phases.inColumnGroup.endTagCol = function(name) {
		parser.parse_error("no-end-tag", {name: 'col'});
	};

	phases.inColumnGroup.endTagOther = function(name) {
		var ignoreEndTag = this.ignoreEndTagColgroup();
		this.endTagColgroup('colgroup');
		if (!ignoreEndTag) phase.processEndTag(name) ;
	};

	phases.inForeignContent = Object.create(phases.base);

	phases.inForeignContent.start_tag_handlers = {
		'-default': 'startTagOther'
	};

	phases.inForeignContent.end_tag_handlers = {
		'-default': 'endTagOther'
	};

	phases.inForeignContent.startTagOther = function(name, attributes, self_closing) {
		if (['mglyph', 'malignmark'].indexOf(name) != -1 &&
			['mi', 'mo', 'mn', 'ms', 'mtext'].indexOf(tree.open_elements.last().tagName) != -1 &&
			tree.open_elements.last().namespace == 'math') {
			secondary_phase.processStartTag(name, attributes);
			if (phase == 'inForeignContent') {
				if (tree.open_elements.any(function(e) { return e.namespace; })) {
					phase = secondary_phase;
				}
			}
		} else if (['b', 'big', 'blockquote', 'body', 'br', 'center', 'code', 'dd', 'div', 'dl', 'dt', 'em', 'embed', 'font', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'head', 'hr', 'i', 'img', 'li', 'listing', 'menu', 'meta', 'nobr', 'ol', 'p', 'pre', 'ruby', 's', 'small', 'span', 'strong', 'strike', 'sub', 'sup', 'table', 'tt', 'u', 'ul', 'var'].indexOf(name) != -1) {
			parser.parse_error('html-in-foreign-content', {name: name});
			while(tree.open_elements.last().namespace) {
				tree.open_elements.pop();
			}
			phase = secondary_phase;
			phase.processStartTag(name, attributes);
		} else {
			if (tree.open_elements.last().namespace == 'math') {
				attributes = this.adjust_mathml_attributes(attributes);
			}
			attributes = this.adjust_foreign_attributes(attributes);
			tree.insert_foreign_element(name, attributes, tree.open_elements.last().namespace);
			if (self_closing) tree.open_elements.pop();
		}
	};

	phases.inForeignContent.endTagOther = function(name) {
		secondary_phase.processEndTag(name);
		if (phase == 'inForeignContent') {
			if (tree.open_elements.any(function(e) { return e.namespace; })) {
				phase = secondary_phase;
			}
		}
	};

	phases.inForeignContent.processCharacters = function(characters) {
		tree.insert_text(characters);
	};

	phases.inFrameset = Object.create(phases.base);

	phases.inFrameset.start_tag_handlers = {
		html: 'startTagHtml',
		frameset: 'startTagFrameset',
		frame: 'startTagFrame',
		noframes: 'startTagNoframes',
		"-default": 'startTagOther'
	};

	phases.inFrameset.end_tag_handlers = {
		frameset: 'endTagFrameset',
		noframes: 'endTagNoframes',
		'-default': 'endTagOther'
	};

	phases.inFrameset.processCharacters = function(data) {
		parser.parse_error("unexpected-char-in-frameset");
	};

	phases.inFrameset.startTagFrameset = function(name, attributes) {
		tree.insert_element(name, attributes);
	};

	phases.inFrameset.startTagFrame = function(name, attributes) {
		tree.insert_element(name, attributes);
		tree.pop_element();
	};

	phases.inFrameset.startTagNoframes = function(name, attributes) {
		phases.inBody.processStartTag(name, attributes);
	};

	phases.inFrameset.startTagOther = function(name, attributes) {
		parser.parse_error("unexpected-start-tag-in-frameset", {name: name});
	};

	phases.inFrameset.endTagFrameset = function(name, attributes) {
		if (tree.open_elements.last().tagName.toLowerCase() == 'html') {
			// inner_html case
			parser.parse_error("unexpected-frameset-in-frameset-innerhtml");
		} else {
			tree.pop_element();
		}

		if (!parser.inner_html && tree.open_elements.last().tagName.toLowerCase() != 'frameset') {
			// If we're not in inner_html mode an the current node is not a "frameset" element (anymore) then switch
			parser.newPhase('afterFrameset');
		}
	};

	phases.inFrameset.endTagNoframes = function(name) {
		phases.inBody.processEndTag(name);
	};

	phases.inFrameset.endTagOther = function(name) {
		parser.parse_error("unexpected-end-tag-in-frameset", {name: name});
	};


	phases.inHead = Object.create(phases.base);

	phases.inHead.start_tag_handlers = {
		html: 'startTagHtml',
		head: 'startTagHead',
		title: 'startTagTitle',
		type: 'startTagType',
		style: 'startTagStyle',
		script: 'startTagScript',
		noscript: 'startTagNoScript',
		base: 'startTagBaseLinkMeta',
		link: 'startTagBaseLinkMeta',
		meta: 'startTagBaseLinkMeta',
		"-default": 'startTagOther'
	};

	phases.inHead.end_tag_handlers = {
		head: 'endTagHead',
		html: 'endTagImplyAfterHead',
		body: 'endTagImplyAfterHead',
		p: 'endTagImplyAfterHead',
		br: 'endTagImplyAfterHead',
		title: 'endTagTitleStyleScriptNoscript',
		style: 'endTagTitleStyleScriptNoscript',
		script: 'endTagTitleStyleScriptNoscript',
		noscript: 'endTagTitleStyleScriptNoscript',
		"-default": 'endTagOther'
	};

	phases.inHead.processEOF = function() {
		var name = tree.open_elements.last().tagName.toLowerCase();
		if (['title', 'style', 'script'].indexOf(name) != -1) {
			parser.parse_error("expected-named-closing-tag-but-got-eof", {name: name});
			tree.pop_element();
		}

		this.anything_else();

		phase.processEOF();
	};

	phases.inHead.processCharacters = function(data) {
		var name = tree.open_elements.last().tagName.toLowerCase();
		HTML5.debug('parser.inHead.processCharacters', data);
		if (['title', 'style', 'script', 'noscript'].indexOf(name) != -1) {
			tree.insert_text(data);
		} else {
			this.anything_else();
			phase.processCharacters(data);
		}
	};

	phases.inHead.startTagHead = function(name, attributes) {
		parser.parse_error('two-heads-are-not-better-than-one');
	};

	phases.inHead.startTagTitle = function(name, attributes) {
		var element = tree.createElement(name, attributes);
		this.appendToHead(element);
		tree.open_elements.push(element);
		parser.tokenizer.content_model = HTML5.Models.RCDATA;
	};

	phases.inHead.startTagStyle = function(name, attributes) {
		if (tree.head_pointer && parser.phaseName == 'inHead') {
			var element = tree.createElement(name, attributes);
			this.appendToHead(element);
			tree.open_elements.push(element);
		} else {
			tree.insert_element(name, attributes);
		}
		parser.tokenizer.content_model = HTML5.Models.CDATA;
	};

	phases.inHead.startTagNoScript = function(name, attributes) {
		// XXX Need to decide whether to implement the scripting disabled case
		var element = tree.createElement(name, attributes);
		if (tree.head_pointer && parser.phaseName == 'inHead') {
			this.appendToHead(element);
		} else {
			tree.open_elements.last().appendChild(element);
		}
		tree.open_elements.push(element);
		parser.tokenizer.content_model = HTML5.Models.CDATA;
	};

	phases.inHead.startTagScript = function(name, attributes) {
		// XXX Inner HTML case may be wrong
		var element = tree.createElement(name, attributes);
		//element.flags.push('parser-inserted');
		if (tree.head_pointer && parser.phaseName == 'inHead') {
			this.appendToHead(element);
		} else {
			tree.open_elements.last().appendChild(element);
		}
		tree.open_elements.push(element);
		parser.tokenizer.content_model = HTML5.Models.SCRIPT_CDATA;
	};

	phases.inHead.startTagBaseLinkMeta = function(name, attributes) {
		var element = tree.createElement(name, attributes);
		if (tree.head_pointer && parser.phaseName == 'inHead') {
			this.appendToHead(element);
		} else {
			tree.open_elements.last().appendChild(element);
		}
	};

	phases.inHead.startTagOther = function(name, attributes) {
		this.anything_else();
		phase.processStartTag(name, attributes);
	};

	phases.inHead.endTagHead = function(name) {
		if (tree.open_elements[tree.open_elements.length - 1].tagName.toLowerCase() == 'head') {
			tree.pop_element();
		} else {
			parser.parse_error('unexpected-end-tag', {name: 'head'});
		}
		parser.newPhase('afterHead');
	};

	phases.inHead.endTagImplyAfterHead = function(name) {
		this.anything_else();
		phase.processEndTag(name);
	};

	phases.inHead.endTagTitleStyleScriptNoscript = function(name) {
		if (tree.open_elements[tree.open_elements.length - 1].tagName.toLowerCase() == name.toLowerCase()) {
			tree.pop_element();
		} else {
			parser.parse_error('unexpected-end-tag', {name: name});
		}
	};

	phases.inHead.endTagOther = function(name) {
		this.anything_else();
	};

	phases.inHead.anything_else = function() {
		if (tree.open_elements.last().tagName.toLowerCase() == 'head') {
			this.endTagHead('head');
		} else {
			parser.newPhase('afterHead');
		}
	};

	// protected

	phases.inHead.appendToHead = function(element) {
		if (!tree.head_pointer) {
			// FIXME assert(parser.inner_html)
			tree.open_elements.last().appendChild(element);
		} else {
			tree.head_pointer.appendChild(element);
		}
	};


	phases.inTable = Object.create(phases.base);

	phases.inTable.start_tag_handlers = {
		html: 'startTagHtml',
		caption: 'startTagCaption',
		colgroup: 'startTagColgroup',
		col: 'startTagCol',
		table: 'startTagTable',
		tbody: 'startTagRowGroup',
		tfoot: 'startTagRowGroup',
		thead: 'startTagRowGroup',
		td: 'startTagImplyTbody',
		th: 'startTagImplyTbody',
		tr: 'startTagImplyTbody',
		'-default': 'startTagOther'
	};

	phases.inTable.end_tag_handlers = {
		table: 'endTagTable',
		body: 'endTagIgnore',
		caption: 'endTagIgnore',
		col: 'endTagIgnore',
		colgroup: 'endTagIgnore',
		html: 'endTagIgnore',
		tbody: 'endTagIgnore',
		td: 'endTagIgnore',
		tfoot: 'endTagIgnore',
		th: 'endTagIgnore',
		thead: 'endTagIgnore',
		tr: 'endTagIgnore',
		'-default': 'endTagOther'
	};

	phases.inTable.processCharacters =  function(data) {
		parser.parse_error("unexpected-char-implies-table-voodoo");
		tree.insert_from_table = true;
		phases.inBody.processCharacters(data);
		tree.insert_from_table = false;
	};

	phases.inTable.startTagCaption = function(name, attributes) {
		this.clearStackToTableContext();
		tree.activeFormattingElements.push(HTML5.Marker);
		tree.insert_element(name, attributes);
		parser.newPhase('inCaption');
	};

	phases.inTable.startTagColgroup = function(name, attributes) {
		this.clearStackToTableContext();
		tree.insert_element(name, attributes);
		parser.newPhase('inColumnGroup');
	};

	phases.inTable.startTagCol = function(name, attributes) {
		this.startTagColgroup('colgroup', {});
		phase.processStartTag(name, attributes);
	};

	phases.inTable.startTagRowGroup = function(name, attributes) {
		this.clearStackToTableContext();
		tree.insert_element(name, attributes);
		parser.newPhase('inTableBody');
	};

	phases.inTable.startTagImplyTbody = function(name, attributes) {
		this.startTagRowGroup('tbody', {});
		phase.processStartTag(name, attributes);
	};

	phases.inTable.startTagTable = function(name, attributes) {
		parser.parse_error("unexpected-start-tag-implies-end-tag",
				{startName: "table", endName: "table"});
		phase.processEndTag('table');
		if (!parser.inner_html) phase.processStartTag(name, attributes);
	};

	phases.inTable.startTagOther = function(name, attributes) {
		this.parse_error("unexpected-start-tag-implies-table-voodoo", {name: name});
		tree.insert_from_table = true;
		phases.inBody.processStartTag(name, attributes);
		tree.insert_from_table = false;
	};

	phases.inTable.endTagTable = function(name) {
		if (this.inScope(name, HTML5.TABLE_SCOPING_ELEMENTS)) {
			tree.generateImpliedEndTags();
			if (tree.open_elements.last().tagName.toLowerCase() != name) {
				parser.parse_error("end-tag-too-early-named", {gotName: 'table', expectedName: tree.open_elements.last().tagName.toLowerCase()});
			}

			tree.remove_open_elements_until('table');
			parser.reset_insertion_mode(tree.open_elements.last());
		} else {
			assert.ok(parser.inner_html);
			parser.parse_error();
		}
	};

	phases.inTable.endTagIgnore = function(name) {
		parser.parse_error("unexpected-end-tag", {name: name});
	};

	phases.inTable.endTagOther = function(name) {
		parser.parse_error("unexpected-end-tag-implies-table-voodoo", {name: name});
		// Make all the special element rearranging voodoo kick in
		tree.insert_from_table = true;
		// Process the end tag in the "in body" mode
		phases.inBody.processEndTag(name);
		tree.insert_from_table = false;
	};

	phases.inTable.clearStackToTableContext = function() {
		var name = tree.open_elements.last().tagName.toLowerCase();
		while (name != 'table' && name != 'html') {
			parser.parse_error("unexpected-implied-end-tag-in-table", {name: name});
			tree.pop_element();
			name = tree.open_elements.last().tagName.toLowerCase();
		}
		// When the current node is <html> it's an inner_html case
	};

	phases.inTableBody = Object.create(phases.base);

	phases.inTableBody.start_tag_handlers = {
		html: 'startTagHtml',
		tr: 'startTagTr',
		td: 'startTagTableCell',
		th: 'startTagTableCell',
		caption: 'startTagTableOther',
		col: 'startTagTableOther',
		colgroup: 'startTagTableOther',
		tbody: 'startTagTableOther',
		tfoot: 'startTagTableOther',
		thead: 'startTagTableOther',
		'-default': 'startTagOther'
	};

	phases.inTableBody.end_tag_handlers = {
		table: 'endTagTable',
		tbody: 'endTagTableRowGroup',
		tfoot: 'endTagTableRowGroup',
		thead: 'endTagTableRowGroup',
		body: 'endTagIgnore',
		caption: 'endTagIgnore',
		col: 'endTagIgnore',
		colgroup: 'endTagIgnore',
		html: 'endTagIgnore',
		td: 'endTagIgnore',
		th: 'endTagIgnore',
		tr: 'endTagIgnore',
		'-default': 'endTagOther'
	};

	phases.inTableBody.processCharacters = function(data) {
		phases.inTable.processCharacters(data);
	};

	phases.inTableBody.startTagTr = function(name, attributes) {
		this.clearStackToTableBodyContext();
		tree.insert_element(name, attributes);
		parser.newPhase('inRow');
	};

	phases.inTableBody.startTagTableCell = function(name, attributes) {
		parser.parse_error("unexpected-cell-in-table-body", {name: name});
		this.startTagTr('tr', {});
		phase.processStartTag(name, attributes);
	};

	phases.inTableBody.startTagTableOther = function(name, attributes) {
		// XXX any ideas on how to share this with endTagTable
		if (this.inScope('tbody', HTML5.TABLE_SCOPING_ELEMENTS) ||  this.inScope('thead', HTML5.TABLE_SCOPING_ELEMENTS) || this.inScope('tfoot', HTML5.TABLE_SCOPING_ELEMENTS)) {
			this.clearStackToTableBodyContext();
			this.endTagTableRowGroup(tree.open_elements.last().tagName.toLowerCase());
			phase.processStartTag(name, attributes);
		} else {
			// inner_html case
			parser.parse_error();
		}
	};
	
	phases.inTableBody.startTagOther = function(name, attributes) {
		phases.inTable.processStartTag(name, attributes);
	};

	phases.inTableBody.endTagTableRowGroup = function(name) {
		if (this.inScope(name, HTML5.TABLE_SCOPING_ELEMENTS)) {
			this.clearStackToTableBodyContext();
			tree.pop_element();
			parser.newPhase('inTable');
		} else {
			parser.parse_error('unexpected-end-tag-in-table-body', {name: name});
		}
	};

	phases.inTableBody.endTagTable = function(name) {
		if (this.inScope('tbody', HTML5.TABLE_SCOPING_ELEMENTS) || this.inScope('thead', HTML5.TABLE_SCOPING_ELEMENTS) || this.inScope('tfoot', HTML5.TABLE_SCOPING_ELEMENTS)) {
			this.clearStackToTableBodyContext();
			this.endTagTableRowGroup(tree.open_elements.last().tagName.toLowerCase());
			phase.processEndTag(name);
		} else {
			// inner_html case
			this.parse_error();
		}
	};

	phases.inTableBody.endTagIgnore = function(name) {
		parser.parse_error("unexpected-end-tag-in-table-body", {name: name});
	};

	phases.inTableBody.endTagOther = function(name) {
		phases.inTable.processEndTag(name);
	};

	phases.inTableBody.clearStackToTableBodyContext = function() {
		var name = tree.open_elements.last().tagName.toLowerCase();
		while(name != 'tbody' && name != 'tfoot' && name != 'thead' && name != 'html') {
			parser.parse_error("unexpected-implied-end-tag-in-table", {name: name});
			tree.pop_element();
			name = tree.open_elements.last().tagName.toLowerCase();
		}
	};

	phases.inSelect = Object.create(phases.base);

	phases.inSelect.start_tag_handlers = {
		html: 'startTagHtml',
		option: 'startTagOption',
		optgroup: 'startTagOptgroup',
		select: 'startTagSelect',
		'-default': 'startTagOther'
	};

	phases.inSelect.end_tag_handlers = {
		option: 'endTagOption',
		optgroup: 'endTagOptgroup',
		select: 'endTagSelect',
		caption: 'endTagTableElements',
		table: 'endTagTableElements',
		tbody: 'endTagTableElements',
		tfoot: 'endTagTableElements',
		thead: 'endTagTableElements',
		tr: 'endTagTableElements',
		td: 'endTagTableElements',
		th: 'endTagTableElements',
		'-default': 'endTagOther'
	};
	
	phases.inSelect.processCharacters = function(data) {
		tree.insert_text(data);
	};

	phases.inSelect.startTagOption = function(name, attributes) {
		// we need to imply </option> if <option> is the current node
		if (tree.open_elements.last().tagName.toLowerCase() == 'option') tree.pop_element();
		tree.insert_element(name, attributes);
	};

	phases.inSelect.startTagOptgroup = function(name, attributes) {
		if (tree.open_elements.last().tagName.toLowerCase() == 'option') tree.pop_element();
		if (tree.open_elements.last().tagName.toLowerCase() == 'optgroup') tree.pop_element();
		tree.insert_element(name, attributes);
	};
	
	phases.inSelect.endTagOption = function(name) {
		if (tree.open_elements.last().tagName.toLowerCase() == 'option') {
			tree.pop_element();
		} else {
			parser.parse_error('unexpected-end-tag-in-select', {name: 'option'});
		}
	};

	phases.inSelect.endTagOptgroup = function(name) {
		// </optgroup> implicitly closes <option>
		if (tree.open_elements.last().tagName.toLowerCase() == 'option' && tree.open_elements[tree.open_elements.length - 2].tagName.toLowerCase() == 'optgroup') {
			tree.pop_element();
		}

		// it also closes </optgroup>
		if (tree.open_elements.last().tagName.toLowerCase() == 'optgroup') {
			tree.pop_element();
		} else {
			// But nothing else
			parser.parse_error('unexpected-end-tag-in-select', {name: 'optgroup'});
		}
	};

	phases.inSelect.startTagSelect = function(name) {
		parser.parse_error("unexpected-select-in-select");
		this.endTagSelect('select');
	};

	phases.inSelect.endTagSelect = function(name) {
		if (this.inScope('select', HTML5.TABLE_SCOPING_ELEMENTS)) {
			tree.remove_open_elements_until('select');
			parser.reset_insertion_mode(tree.open_elements.last());
		} else {
			// inner_html case
			parser.parse_error();
		}
	};

	phases.inSelect.endTagTableElements = function(name) {
		parser.parse_error('unexpected-end-tag-in-select', {name: name});
		
		if (this.inScope(name, HTML5.TABLE_SCOPING_ELEMENTS)) {
			this.endTagSelect('select');
			phase.processEndTag(name);
		}
	};

	phases.inSelect.startTagOther = function(name, attributes) {
		parser.parse_error("unexpected-start-tag-in-select", {name: name});
	};

	phases.inSelect.endTagOther = function(name) {
		parser.parse_error('unexpected-end-tag-in-select', {name: name});
	};

	phases.inSelectInTable = Object.create(phases.base);

	phases.inSelectInTable.start_tag_handlers = {
		caption: 'startTagTable',
		table: 'startTagTable',
		tbody: 'startTagTable',
		tfoot: 'startTagTable',
		thead: 'startTagTable',
		tr: 'startTagTable',
		td: 'startTagTable',
		th: 'startTagTable',
		'-default': 'startTagOther'
	};

	phases.inSelectInTable.end_tag_handlers = {
		caption: 'endTagTable',
		table: 'endTagTable',
		tbody: 'endTagTable',
		tfoot: 'endTagTable',
		thead: 'endTagTable',
		tr: 'endTagTable',
		td: 'endTagTable',
		th: 'endTagTable',
		'-default': 'endTagOther'
	};

	phases.inSelectInTable.processCharacters = function(data) {
		phases.inSelect.processCharacters(data);
	};

	phases.inSelectInTable.startTagTable = function(name, attributes) {
		parser.parse_error("unexpected-table-element-start-tag-in-select-in-table", {name: name});
		this.endTagOther("select");
		phase.processStartTag(name, attributes);
	};

	phases.inSelectInTable.startTagOther = function(name, attributes) {
		phases.inSelect.processStartTag(name, attributes);
	};

	phases.inSelectInTable.endTagTable = function(name) {
		parser.parse_error("unexpected-table-element-end-tag-in-select-in-table", {name: name});
		if (this.inScope(name, HTML5.TABLE_SCOPING_ELEMENTS)) {
			this.endTagOther("select");
			phase.processEndTag(name);
		}
	};

	phases.inSelectInTable.endTagOther = function(name) {
		phases.inSelect.processEndTag(name);
	};

	phases.inRow = Object.create(phases.base);

	phases.inRow.start_tag_handlers = {
		html: 'startTagHtml',
		td: 'startTagTableCell',
		th: 'startTagTableCell',
		caption: 'startTagTableOther',
		col: 'startTagTableOther',
		colgroup: 'startTagTableOther',
		tbody: 'startTagTableOther',
		tfoot: 'startTagTableOther',
		thead: 'startTagTableOther',
		tr: 'startTagTableOther',
		'-default': 'startTagOther'
	};

	phases.inRow.end_tag_handlers = {
		tr: 'endTagTr',
		table: 'endTagTable',
		tbody: 'endTagTableRowGroup',
		tfoot: 'endTagTableRowGroup',
		thead: 'endTagTableRowGroup',
		body: 'endTagIgnore',
		caption: 'endTagIgnore',
		col: 'endTagIgnore',
		colgroup: 'endTagIgnore',
		html: 'endTagIgnore',
		td: 'endTagIgnore',
		th: 'endTagIgnore',
		'-default': 'endTagOther'
	};

	phases.inRow.processCharacters = function(data) {
		phases.inTable.processCharacters(data);
	};

	phases.inRow.startTagTableCell = function(name, attributes) {
		this.clearStackToTableRowContext();
		tree.insert_element(name, attributes);
		parser.newPhase('inCell');
		tree.activeFormattingElements.push(HTML5.Marker);
	};

	phases.inRow.startTagTableOther = function(name, attributes) {
		var ignoreEndTag = this.ignoreEndTagTr();
		this.endTagTr('tr');
		// XXX how are we sure it's always ignored in the inner_html case?
		if (!ignoreEndTag) phase.processStartTag(name, attributes);
	};

	phases.inRow.startTagOther = function(name, attributes) {
		phases.inTable.processStartTag(name, attributes);
	};

	phases.inRow.endTagTr = function(name) {
		if (this.ignoreEndTagTr()) {
			assert.ok(parser.inner_html);
			parser.parse_error();
		} else {
			this.clearStackToTableRowContext();
			tree.pop_element();
			parser.newPhase('inTableBody');
		}
	};

	phases.inRow.endTagTable = function(name) {
		var ignoreEndTag = this.ignoreEndTagTr();
		this.endTagTr('tr');
		// Reprocess the current tag if the tr end tag was not ignored
		// XXX how are we sure it's always ignored in the inner_html case?
		if (!ignoreEndTag) phase.processEndTag(name);
	};

	phases.inRow.endTagTableRowGroup = function(name) {
		if (this.inScope(name, HTML5.TABLE_SCOPING_ELEMENTS)) {
			this.endTagTr('tr');
			phase.processEndTag(name);
		} else {
			// inner_html case
			parser.parse_error();
		}
	};

	phases.inRow.endTagIgnore = function(name) {
		parser.parse_error("unexpected-end-tag-in-table-row", {name: name});
	};

	phases.inRow.endTagOther = function(name) {
		phases.inTable.processEndTag(name);
	};

	phases.inRow.clearStackToTableRowContext = function() {
		var name = tree.open_elements.last().tagName.toLowerCase();
		while (name != 'tr' && name != 'html') {
			parser.parse_error("unexpected-implied-end-tag-in-table-row", {name: name});
			tree.pop_element();
			name = tree.open_elements.last().tagName.toLowerCase();
		}
	};

	phases.inRow.ignoreEndTagTr = function() {
		return !this.inScope('tr', HTML5.TABLE_SCOPING_ELEMENTS);
	};

	phases.rootElement = Object.create(phases.base);

	phases.rootElement.processEOF = function() {
		this.insert_html_element();
		phase.processEOF();
	};

	phases.rootElement.processComment = function(data) {
		tree.insert_comment(data, this.tree.document);
	};

	phases.rootElement.processSpaceCharacters = function(data) {
	};

	phases.rootElement.processCharacters = function(data) {
		this.insert_html_element();
		phase.processCharacters(data);
	};

	phases.rootElement.processStartTag = function(name, attributes) {
		if (name == 'html') parser.first_start_tag = true;
		this.insert_html_element();
		phase.processStartTag(name, attributes);
	};

	phases.rootElement.processEndTag = function(name) {
		this.insert_html_element();
		phase.processEndTag(name);
	};

	phases.rootElement.insert_html_element = function() {
		var element = tree.createElement('html', {});
		tree.open_elements.push(element);
		tree.document.appendChild(element);
		parser.newPhase('beforeHead');
	};

	phases.trailingEnd = Object.create(phases.base);

	phases.trailingEnd.processEOF = function() {};

	phases.trailingEnd.processComment = function(data) {
		tree.insert_comment(data);
	};

	phases.trailingEnd.processSpaceCharacters = function(data) {
		parser.last_phase.processSpaceCharacters(data);
	};

	phases.trailingEnd.processCharacters = function(data) {
		parser.parse_error('expected-eof-but-got-char');
		phase = parser.last_phase;
		phase.processCharacters(data);
	};

	phases.trailingEnd.processStartTag = function(name, attributes) {
		parser.parse_error('expected-eof-but-got-start-tag');
		phase = parser.last_phase;
		phase.processStartTag(name, attributes);
	};

	phases.trailingEnd.processEndTag = function(name, attributes) {
		parser.parse_error('expected-eof-but-got-end-tag');
		phase = parser.last_phase;
		phase.processEndTag(name);
	};

	phases.inBody = Object.create(phases.base);

	phases.inBody.start_tag_handlers = {
		html: 'startTagHtml',
		head: 'startTagMisplaced',
		base: 'startTagProcessInHead',
		link: 'startTagProcessInHead',
		meta: 'startTagProcessInHead',
		script: 'startTagProcessInHead',
		style: 'startTagProcessInHead',
		title: 'startTagTitle',
		body: 'startTagBody',
		form: 'startTagForm',
		plaintext: 'startTagPlaintext',
		a: 'startTagA',
		button: 'startTagButton',
		xmp: 'startTagXmp',
		table: 'startTagTable',
		hr: 'startTagHr',
		image: 'startTagImage',
		input: 'startTagInput',
		textarea: 'startTagTextarea',
		select: 'startTagSelect',
		isindex: 'startTagIsindex',
		applet:	'startTagAppletMarqueeObject',
		marquee:	'startTagAppletMarqueeObject',
		object:	'startTagAppletMarqueeObject',
		li: 'startTagListItem',
		dd: 'startTagListItem',
		dt: 'startTagListItem',
		address: 'startTagCloseP',
		blockquote: 'startTagCloseP',
		center: 'startTagCloseP',
		dir: 'startTagCloseP',
		div: 'startTagCloseP',
		dl: 'startTagCloseP',
		fieldset: 'startTagCloseP',
		listing: 'startTagCloseP',
		menu: 'startTagCloseP',
		ol: 'startTagCloseP',
		p: 'startTagCloseP',
		pre: 'startTagCloseP', /// @todo: Handle <pre> and <listing> specially with regards to newlines
		ul: 'startTagCloseP',
		b: 'startTagFormatting',
		big: 'startTagFormatting',
		em: 'startTagFormatting',
		font: 'startTagFormatting',
		i: 'startTagFormatting',
		s: 'startTagFormatting',
		small: 'startTagFormatting',
		strike: 'startTagFormatting',
		strong: 'startTagFormatting',
		tt: 'startTagFormatting',
		u: 'startTagFormatting',
		nobr: 'startTagNobr',
		area: 'startTagVoidFormatting',
		basefont: 'startTagVoidFormatting',
		bgsound: 'startTagVoidFormatting',
		br: 'startTagVoidFormatting',
		embed: 'startTagVoidFormatting',
		img: 'startTagVoidFormatting',
		param: 'startTagVoidFormatting',
		spacer: 'startTagVoidFormatting',
		wbr: 'startTagVoidFormatting',
		iframe: 'startTagCdata',
		noembed: 'startTagCdata',
		noframes: 'startTagCdata',
		noscript: 'startTagCdata',
		h1: 'startTagHeading',
		h2: 'startTagHeading',
		h3: 'startTagHeading',
		h4: 'startTagHeading',
		h5: 'startTagHeading',
		h6: 'startTagHeading',
		caption: 'startTagMisplaced',
		col: 'startTagMisplaced',
		colgroup: 'startTagMisplaced',
		frame: 'startTagMisplaced',
		frameset: 'startTagMisplaced',
		tbody: 'startTagMisplaced',
		td: 'startTagMisplaced',
		tfoot: 'startTagMisplaced',
		th: 'startTagMisplaced',
		thead: 'startTagMisplaced',
		tr: 'startTagMisplaced',
		option: 'startTagMisplaced',
		optgroup: 'startTagMisplaced',
		'event-source': 'startTagNew',
		section: 'startTagNew',
		nav: 'startTagNew',
		article: 'startTagNew',
		aside: 'startTagNew',
		header: 'startTagNew',
		footer: 'startTagNew',
		datagrid: 'startTagNew',
		command: 'startTagNew',
		math: 'startTagMath',
		svg: 'startTagSVG',
		rt: 'startTagRpRt',
		rp: 'startTagRpRt',
		"-default": 'startTagOther'
	};

	phases.inBody.end_tag_handlers = {
		p: 'endTagP',
		body: 'endTagBody',
		html: 'endTagHtml',
		form: 'endTagForm',
		applet: 'endTagAppletButtonMarqueeObject',
		button: 'endTagAppletButtonMarqueeObject',
		marquee: 'endTagAppletButtonMarqueeObject',
		object: 'endTagAppletButtonMarqueeObject',
		dd: 'endTagListItem',
		dt: 'endTagListItem',
		li: 'endTagListItem',
		address: 'endTagBlock',
		blockquote: 'endTagBlock',
		center: 'endTagBlock',
		div: 'endTagBlock',
		dl: 'endTagBlock',
		fieldset: 'endTagBlock',
		listing: 'endTagBlock',
		menu: 'endTagBlock',
		ol: 'endTagBlock',
		pre: 'endTagBlock',
		ul: 'endTagBlock',
		h1: 'endTagHeading',
		h2: 'endTagHeading',
		h3: 'endTagHeading',
		h4: 'endTagHeading',
		h5: 'endTagHeading',
		h6: 'endTagHeading',
		a: 'endTagFormatting',
		b: 'endTagFormatting',
		big: 'endTagFormatting',
		em: 'endTagFormatting',
		font: 'endTagFormatting',
		i: 'endTagFormatting',
		nobr: 'endTagFormatting',
		s: 'endTagFormatting',
		small: 'endTagFormatting',
		strike: 'endTagFormatting',
		strong: 'endTagFormatting',
		tt: 'endTagFormatting',
		u: 'endTagFormatting',
		head: 'endTagMisplaced',
		frameset: 'endTagMisplaced',
		select: 'endTagMisplaced',
		optgroup: 'endTagMisplaced',
		option: 'endTagMisplaced',
		table: 'endTagMisplaced',
		caption: 'endTagMisplaced',
		colgroup: 'endTagMisplaced',
		col: 'endTagMisplaced',
		thead: 'endTagMisplaced',
		tfoot: 'endTagMisplaced',
		tbody: 'endTagMisplaced',
		tr: 'endTagMisplaced',
		td: 'endTagMisplaced',
		th: 'endTagMisplaced',
		br: 'endTagBr',
		area: 'endTagNone',
		basefont: 'endTagNone',
		bgsound: 'endTagNone',
		embed: 'endTagNone',
		hr: 'endTagNone',
		image: 'endTagNone',
		img: 'endTagNone',
		input: 'endTagNone',
		isindex: 'endTagNone',
		param: 'endTagNone',
		spacer: 'endTagNone',
		wbr: 'endTagNone',
		frame: 'endTagNone',
		noframes:	'endTagCdataTextAreaXmp',
		noscript:	'endTagCdataTextAreaXmp',
		noembed:	'endTagCdataTextAreaXmp',
		textarea:	'endTagCdataTextAreaXmp',
		xmp:	'endTagCdataTextAreaXmp',
		iframe:	'endTagCdataTextAreaXmp',
		'event-source': 'endTagNew',
		section: 'endTagNew',
		nav: 'endTagNew',
		article: 'endTagNew',
		aside: 'endTagNew',
		header: 'endTagNew',
		footer: 'endTagNew',
		datagrid: 'endTagNew',
		command: 'endTagNew',
		title: 'endTagTitle',
		"-default": 'endTagOther'
	};

	phases.inBody.processSpaceCharactersDropNewline = function(data) {
		this.dropNewline = false;
		var lastTag = tree.open_elements.last().tagName.toLowerCase();
		if (data.length > 0 && data[0] == "\n" && ('pre' == lastTag || 'textarea' == lastTag) && !tree.open_elements.last().hasChildNodes()) {
			data = data.slice(1);
		}

		if (data.length > 0) {
			tree.reconstructActiveFormattingElements();
			tree.insert_text(data);
		}
	};

	phases.inBody.processSpaceCharacters = function(data) {
		if (this.dropNewline) {
			this.processSpaceCharactersDropNewline(data);
		} else {
			this.processSpaceCharactersNonPre(data);
		}
	};

	phases.inBody.processSpaceCharactersNonPre = function(data) {
		tree.reconstructActiveFormattingElements();
		tree.insert_text(data);
	};

	phases.inBody.processCharacters = function(data) {
		// XXX The specification says to do this for every character at the moment,
		// but apparently that doesn't match the real world so we don't do it for
		// space characters.
		tree.reconstructActiveFormattingElements();
		tree.insert_text(data);
	};

	phases.inBody.startTagProcessInHead = function(name, attributes) {
		phases.inHead.processStartTag(name, attributes);
	};

	phases.inBody.startTagBody = function(name, attributes) {
		parser.parse_error('unexpected-start-tag', {name: 'body'});
		if (tree.open_elements.length == 1 ||
			tree.open_elements[1].tagName.toLowerCase() != 'body') {
			assert.ok(parser.inner_html);
		} else {
			for(var i = 0; i < attributes.length; i++) {
				if (!tree.open_elements[1].getAttribute(attributes[i].nodeName)) {
					tree.open_elements[1].setAttribute(attributes[i].nodeName, attributes[i].nodeValue);
				}
			}
		}
	};

	phases.inBody.startTagCloseP = function(name, attributes) {
		if (this.inScope('p', HTML5.BUTTON_SCOPING_ELEMENTS)) this.endTagP('p');
		tree.insert_element(name, attributes);
		if (name == 'pre') {
			this.dropNewline = true;
		}
	};

	phases.inBody.startTagForm = function(name, attributes) {
		if (tree.formPointer) {
			parser.parse_error('unexpected-start-tag', {name: name});
		} else {
			if (this.inScope('p', HTML5.BUTTON_SCOPING_ELEMENTS)) this.endTagP('p');
			tree.insert_element(name, attributes);
			tree.formPointer = tree.open_elements.last();
		}
	};

	phases.inBody.startTagRpRt = function(name, attributes) {
		if (this.inScope('ruby')) {
			tree.generateImpliedEndTags();
			if (tree.open_elements.last().tagName.toLowerCase() != 'ruby') {
				parser.parse_error('unexpected child of ruby');
				while(tree.open_elements.last().tagName.toLowerCase() != 'ruby') tree.pop_element();
			}
		}
		this.startTagOther(name, attributes);
	};

	phases.inBody.startTagListItem = function(name, attributes) {
		/// @todo: Fix according to current spec. http://www.w3.org/TR/html5/tree-construction.html#parsing-main-inbody
		var stopNames = {li: ['li'], dd: ['dd', 'dt'], dt: ['dd', 'dt']};
		var stopName = stopNames[name];

		function getName(n) {
			return n.name;
		}

		var els = tree.open_elements;
		for(var i = els.length - 1; i >= 0; i--) {
			var node = els[i];
			if (stopName.indexOf(node.tagName.toLowerCase()) != -1) {
				var poppedNodes = [];
				while(els.length - 1 >= i) {
					poppedNodes.push(els.pop());
				}
				if (poppedNodes.length >= 1) {
					parser.parse_error(poppedNodes.length == 1 ? "missing-end-tag" : "missing-end-tags",
						{name: poppedNodes.slice(0).map(getName).join(', ')});
				}
				break;
			}

			// Phrasing eliments are all non special, non scoping, non
			// formatting elements
			if (HTML5.SPECIAL_ELEMENTS.concat(HTML5.SCOPING_ELEMENTS).indexOf(node.tagName.toLowerCase()) != -1 && (node.tagName.toLowerCase() != 'address' && node.tagName.toLowerCase() != 'div')) break;
		}
		if (this.inScope('p', HTML5.BUTTON_SCOPING_ELEMENTS)) this.endTagP('p');

		// Always insert an <li> element
		tree.insert_element(name, attributes);
	};

	phases.inBody.startTagPlaintext = function(name, attributes) {
		if (this.inScope('p', HTML5.BUTTON_SCOPING_ELEMENTS)) this.endTagP('p');
		tree.insert_element(name, attributes);
		parser.tokenizer.content_model = HTML5.Models.PLAINTEXT;
	};

	phases.inBody.startTagHeading = function(name, attributes) {
		if (this.inScope('p', HTML5.BUTTON_SCOPING_ELEMENTS)) this.endTagP('p');
		if (['h1', 'h2', 'h3', 'h4', 'h5', 'h6'].indexOf(tree.open_elements.last().tagName) != -1) {
			parser.parse_error('heading in heading');
			tree.pop_element();
		}

		tree.insert_element(name, attributes);
	};

	phases.inBody.startTagA = function(name, attributes) {
		var afeAElement = tree.elementInActiveFormattingElements('a');
		if (afeAElement) {
			parser.parse_error("unexpected-start-tag-implies-end-tag", {startName: "a", endName: "a"});
			this.endTagFormatting('a');
			var pos;
			pos = tree.open_elements.indexOf(afeAElement);
			if (pos != -1) tree.open_elements.splice(pos, 1);
			pos = tree.activeFormattingElements.indexOf(afeAElement);
			if (pos != -1) tree.activeFormattingElements.splice(pos, 1);
		}
		tree.reconstructActiveFormattingElements();
		this.addFormattingElement(name, attributes);
	};

	phases.inBody.startTagFormatting = function(name, attributes) {
		tree.reconstructActiveFormattingElements();
		this.addFormattingElement(name, attributes);
	};

	phases.inBody.startTagNobr = function(name, attributes) {
		tree.reconstructActiveFormattingElements();
		if (this.inScope('nobr')) {
			parser.parse_error("unexpected-start-tag-implies-end-tag", {startName: 'nobr', endName: 'nobr'});
			this.processEndTag('nobr');
		}
		this.addFormattingElement(name, attributes);
	};

	phases.inBody.startTagButton = function(name, attributes) {
		if (this.inScope('button')) {
			parser.parse_error('unexpected-start-tag-implies-end-tag', {startName: 'button', endName: 'button'});
			this.processEndTag('button');
			phase.processStartTag(name, attributes);
		} else {
			tree.reconstructActiveFormattingElements();
			tree.insert_element(name, attributes);
			/// @todo set frameset-ok flag to false
			/// @todo is the marker needed anymore?
			tree.activeFormattingElements.push(HTML5.Marker);
		}
	};

	phases.inBody.startTagAppletMarqueeObject = function(name, attributes) {
		tree.reconstructActiveFormattingElements();
		tree.insert_element(name, attributes);
		tree.activeFormattingElements.push(HTML5.Marker);
	};

	phases.inBody.endTagAppletButtonMarqueeObject = function(name) {
		if (!this.inScope(name)) {
			parser.parse_error("unexpected-end-tag", {name: name});
		} else {
			tree.generateImpliedEndTags();
			if (tree.open_elements.last().tagName.toLowerCase() != name) {
				parser.parse_error('end-tag-too-early', {name: name});
			}
			tree.remove_open_elements_until(name);
			tree.clearActiveFormattingElements();
		}
	};

	phases.inBody.startTagXmp = function(name, attributes) {
		tree.reconstructActiveFormattingElements();
		tree.insert_element(name, attributes);
		parser.tokenizer.content_model = HTML5.Models.CDATA;
	};

	phases.inBody.startTagTable = function(name, attributes) {
		if (this.inScope('p')) this.processEndTag('p');
		tree.insert_element(name, attributes);
		parser.newPhase('inTable');
	};

	phases.inBody.startTagVoidFormatting = function(name, attributes) {
		tree.reconstructActiveFormattingElements();
		tree.insert_element(name, attributes);
		tree.pop_element();
	};

	phases.inBody.startTagHr = function(name, attributes) {
		if (this.inScope('p')) this.endTagP('p');
		tree.insert_element(name, attributes);
		tree.pop_element();
	};

	phases.inBody.startTagImage = function(name, attributes) {
		// No, really...
		parser.parse_error('unexpected-start-tag-treated-as', {originalName: 'image', newName: 'img'});
		this.processStartTag('img', attributes);
	};

	phases.inBody.startTagInput = function(name, attributes) {
		tree.reconstructActiveFormattingElements();
		tree.insert_element(name, attributes);
		if (tree.formPointer) {
			// XXX Not sure what to do here
		}
		tree.pop_element();
	};

	phases.inBody.startTagIsindex = function(name, attributes) {
		parser.parse_error('deprecated-tag', {name: 'isindex'});
		if (tree.formPointer) return;
		this.processStartTag('form');
		this.processStartTag('hr');
		this.processStartTag('p');
		this.processStartTag('label');
		this.processCharacters("This is a searchable index. Insert your search keywords here: ");
		attributes.push({nodeName: 'name', nodeValue: 'isindex'});
		this.processStartTag('input', attributes);
		this.processEndTag('label');
		this.processEndTag('p');
		this.processStartTag('hr');
		this.processEndTag('form');
	};

	phases.inBody.startTagTextarea = function(name, attributes) {
		// XXX Form element pointer checking here as well...
		tree.insert_element(name, attributes);
		parser.tokenizer.content_model = HTML5.Models.RCDATA;
		this.dropNewline = true;
	};

	phases.inBody.startTagCdata = function(name, attributes) {
		tree.insert_element(name, attributes);
		parser.tokenizer.content_model = HTML5.Models.CDATA;
	};

	phases.inBody.startTagSelect = function(name, attributes) {
		tree.reconstructActiveFormattingElements();
		tree.insert_element(name, attributes);
		
		var phaseName = parser.phaseName;
		if (phaseName == 'inTable' ||
			phaseName == 'inCaption' ||
			phaseName == 'inColumnGroup' ||
			phaseName == 'inTableBody' ||
			phaseName == 'inRow' ||
			phaseName == 'inCell') {
			parser.newPhase('inSelectInTable');
		} else {
			parser.newPhase('inSelect');
		}
	};

	phases.inBody.startTagMisplaced = function(name, attributes) {
		parser.parse_error('unexpected-start-tag-ignored', {name: name});
	};

	phases.inBody.endTagMisplaced = function(name) {
		// This handles elements with end tags in other insertion modes.
		parser.parse_error("unexpected-end-tag", {name: name});
	};

	phases.inBody.endTagBr = function(name) {
		parser.parse_error("unexpected-end-tag-treated-as", {originalName: "br", newName: "br element"});
		tree.reconstructActiveFormattingElements();
		tree.insert_element(name, []);
		tree.pop_element();
	};

	phases.inBody.startTagOptionOptgroup = function(name, attributes) {
		if (this.inScope('option')) this.endTagOther('option');
		tree.reconstructActiveFormattingElements();
		tree.insert_element(name, attributes);
	};

	phases.inBody.startTagNew = function(name, attributes) {	
		this.startTagOther(name, attributes);
	};

	phases.inBody.startTagOther = function(name, attributes) {
		tree.reconstructActiveFormattingElements();
		tree.insert_element(name, attributes);
	};

	phases.inBody.startTagTitle = function(name, attributes) {
		tree.insert_element(name, attributes);
		parser.tokenizer.content_model = HTML5.Models.RCDATA;
	};

	phases.inBody.endTagTitle = function(name, attributes) {
		if (tree.open_elements[tree.open_elements.length - 1].tagName.toLowerCase() == name.toLowerCase()) {
			tree.pop_element();
			parser.tokenizer.content_model = HTML5.Models.PCDATA;
		} else {
			parser.parse_error('unexpected-end-tag', {name: name});
		}
	};

	phases.inBody.endTagOther = function endTagOther(name) {
		var currentNode;
		function isCurrentNode(el) {
			return el == currentNode;
		}

		var nodes = tree.open_elements;
		for(var eli = nodes.length - 1; eli > 0; eli--) {
			currentNode = nodes[eli];
			if (nodes[eli].tagName.toLowerCase() == name) {
				tree.generateImpliedEndTags();
				if (tree.open_elements.last().tagName.toLowerCase() != name) {
					parser.parse_error('unexpected-end-tag', {name: name});
				}
				
				tree.remove_open_elements_until(isCurrentNode);

				break;
			} else {

				if (HTML5.SPECIAL_ELEMENTS.concat(HTML5.SCOPING_ELEMENTS).indexOf(nodes[eli].tagName.toLowerCase()) != -1) {
					parser.parse_error('unexpected-end-tag', {name: name});
					break;
				}
			}
		}
	};

	phases.inBody.startTagMath = function(name, attributes) {
		tree.reconstructActiveFormattingElements();
		attributes = this.adjust_mathml_attributes(attributes);
		attributes = this.adjust_foreign_attributes(attributes);
		tree.insert_foreign_element(name, attributes, 'math');
		if (false) {
			// If the token has its self-closing flag set, pop the current node off
			// the stack of open elements and acknowledge the token's self-closing flag
		} else {
			secondary_phase = parser.phase;
			parser.newPhase('inForeignContent');
		}
	};

	phases.inBody.startTagSVG = function(name, attributes) {
		tree.reconstructActiveFormattingElements();
		attributes = this.adjust_svg_attributes(attributes);
		attributes = this.adjust_foreign_attributes(attributes);
		tree.insert_foreign_element(name, attributes, 'svg');
		if (false) {
			// If the token has its self-closing flag set, pop the current node off
			// the stack of open elements and acknowledge the token's self-closing flag
		} else {
			secondary_phase = parser.phase;
			parser.newPhase('inForeignContent');
		}
	};

	phases.inBody.endTagP = function(name) {
		if (!this.inScope('p', HTML5.BUTTON_SCOPING_ELENENTS)) {
			parser.parse_error('unexpected-end-tag', {name: 'p'});
			this.startTagCloseP('p', {});
			this.endTagP('p');
		} else {
			tree.generateImpliedEndTags('p');
			if (tree.open_elements.last().tagName.toLowerCase() != 'p')
				parser.parse_error('unexpected-end-tag', {name: 'p'});
			tree.remove_open_elements_until(name);
		}
	};

	phases.inBody.endTagBody = function(name) {
		if (!this.inScope('body')) {
			parser.parse_error('unexpected-end-tag', {name: 'body'});
			return;
		}

		/// @todo Emit parse error on end tags other than the ones listed in http://www.w3.org/TR/html5/tree-construction.html#parsing-main-inbody
		if (tree.open_elements.last().tagName.toLowerCase() != 'body') {
			parser.parse_error('expected-one-end-tag-but-got-another', {
				expectedName: 'body',
				gotName: tree.open_elements.last().tagName.toLowerCase()
			});
		}
		parser.newPhase('afterBody');
	};

	phases.inBody.endTagHtml = function(name) {
		this.endTagBody(name);
		if (!this.inner_html) phase.processEndTag(name);
	};

	phases.inBody.endTagBlock = function(name) {
		if (!this.inScope(name)) {
			parser.parse_error('end-tag-for-tag-not-in-scope', {name: name});
		} else {
			tree.generateImpliedEndTags();
			if (tree.open_elements.last().tagName.toLowerCase() != 'name') {
				parser.parse_error('end-tag-too-early', {name: name});
			}
			tree.remove_open_elements_until(name);
		}
	};

	phases.inBody.endTagForm = function(name)  {
		var node = tree.formPointer;
		tree.formPointer = null;
		if (!node || !this.inScope(name)) {
			parser.parse_error('end-tag-for-tag-not-in-scope', {name: name});
		} else {
			tree.generateImpliedEndTags();
		
			if (tree.open_elements.last().tagName.toLowerCase() != name) {
				parser.parse_error('end-tag-too-early-ignored', {name: 'form'});
			} else {
				tree.pop_element();
				/// @todo the spec is a bit vague here. Remove node from the stack -- what if it's in the middle?
			}
		}
	};

	phases.inBody.endTagListItem = function(name) {
		if (!this.inScope(name, HTML5.LIST_SCOPING_ELEMENTS)) {
			parser.parse_error('wrong-end-tag-ignored', {name: name});
		} else {
			tree.generateImpliedEndTags(name);
			if (tree.open_elements.last().tagName.toLowerCase() != name)
				parser.parse_error('end-tag-too-early', {name: name});
			tree.remove_open_elements_until(name);
		}
	};

	phases.inBody.endTagHeading = function(name) {
		var error = true;
		var i;

		for(i in HTML5.HEADING_ELEMENTS) {
			var el = HTML5.HEADING_ELEMENTS[i];
			if (this.inScope(el)) {
				error = false;
				break;
			}
		}
		if (error) {
			parser.parse_error('wrong-end-tag-ignored', {name: name});
			return;
		}

		tree.generateImpliedEndTags();

		if (tree.open_elements.last().tagName.toLowerCase() != name)
			parser.parse_error('end-tag-too-early', {name: name});

		tree.remove_open_elements_until(function(e) {
			return HTML5.HEADING_ELEMENTS.indexOf(e.tagName.toLowerCase()) != -1;
		});
	};

	phases.inBody.endTagFormatting = function(name) {
		var element;
		var afeElement;

		function isAfeElement(el) {
			return el == afeElement;
		}

		while (true) {
			afeElement = tree.elementInActiveFormattingElements(name);
			if (!afeElement || (tree.open_elements.indexOf(afeElement) != -1 &&
				!this.inScope(afeElement.tagName.toLowerCase()))) {
				parser.parse_error('adoption-agency-1.1', {name: name});
				return;
			} else if (tree.open_elements.indexOf(afeElement) == -1) {
				parser.parse_error('adoption-agency-1.2', {name: name});
				tree.activeFormattingElements.splice(tree.activeFormattingElements.indexOf(afeElement), 1);
				return;
			}

			if (afeElement != tree.open_elements.last()) {
				parser.parse_error('adoption-agency-1.3', {name: name});
			}
			
			// Start of the adoption agency algorithm proper
			var afeIndex = tree.open_elements.indexOf(afeElement);
			var furthestBlock = null;
			var els = tree.open_elements.slice(afeIndex);
			var len = els.length;
			for (var i = 0; i < len; i++) {
				element = els[i];
				if (HTML5.SPECIAL_ELEMENTS.concat(HTML5.SCOPING_ELEMENTS).indexOf(element.tagName.toLowerCase()) != -1) {
					furthestBlock = element;
					break;
				}
			}
			
			if (!furthestBlock) {
				element = tree.remove_open_elements_until(isAfeElement);
				tree.activeFormattingElements.splice(tree.activeFormattingElements.indexOf(element), 1);
				return;
			}


			var commonAncestor = tree.open_elements[afeIndex - 1];

			var bookmark = tree.activeFormattingElements.indexOf(afeElement);

			var lastNode;
			var node;
			var clone;
			lastNode = node = furthestBlock;

			while(true) {
				node = tree.open_elements[tree.open_elements.indexOf(node) - 1];
				while(tree.activeFormattingElements.indexOf(node) == -1) {
					var tmpNode = node;
					node = tree.open_elements[tree.open_elements.indexOf(node) - 1];
					tree.open_elements.splice(tree.open_elements.indexOf(tmpNode), 1);
				}

				if (node == afeElement) break;

				if (lastNode == furthestBlock) {
					bookmark = tree.activeFormattingElements.indexOf(node) + 1;
				}

				var cite = node.parentNode;

				if (node.hasChildNodes()) {
					clone = node.cloneNode();
					tree.activeFormattingElements[tree.activeFormattingElements.indexOf(node)] = clone;
					tree.open_elements[tree.open_elements.indexOf(node)] = clone;
					node = clone;
				}

				if (lastNode.parent) lastNode.parent.removeChild(lastNode);
				node.appendChild(lastNode);
				lastNode = node;

			}

			if (lastNode.parent) lastNode.parent.removeChild(lastNode);
			commonAncestor.appendChild(lastNode);

			clone = afeElement.cloneNode();

			tree.reparentChildren(furthestBlock, clone);

			furthestBlock.appendChild(clone);

			tree.activeFormattingElements.splice(tree.activeFormattingElements.indexOf(afeElement), 1);
			tree.activeFormattingElements.splice(Math.min(bookmark, tree.activeFormattingElements.length), 0, clone);

			tree.open_elements.splice(tree.open_elements.indexOf(afeElement), 1);
			tree.open_elements.splice(tree.open_elements.indexOf(furthestBlock) + 1, 0, clone);

		}
	};

	phases.inBody.addFormattingElement = function(name, attributes) {
		tree.insert_element(name, attributes);
		tree.activeFormattingElements.push(tree.open_elements.last());
	};

	if (options) for(var o in options) {
		this[o] = options[o];
	}

	if (!this.document) {
		var l3, jsdom;
		jsdom = require('jsdom');
		l3 = jsdom.dom.level3.core;
		var DOM = jsdom.browserAugmentation(l3);
		this.document = new DOM.Document('html');
	}

	var tree = this.tree = new HTML5.TreeBuilder(this.document);
};

util.inherits(Parser, events.EventEmitter);

Parser.prototype.parse = function(source) {
	if (!source) throw(new Error("No source to parse"));
	HTML5.debug('parser.parse', source);
	this.tokenizer = new HTML5.Tokenizer(source, this.document, this.tree);
	this.setup();
	this.tokenizer.tokenize();
};

Parser.prototype.parse_fragment = function(source, element) {
	HTML5.debug('parser.parse_fragment', source, element);
	// FIXME: Check to make sure element is inside document
	this.tokenizer = new HTML5.Tokenizer(source, this.document);
	if (element && element.ownerDocument) {
		this.setup(element.tagName, null);
		this.tree.open_elements.push(element);
		this.tree.root_pointer = element;
	} else if (element) {
		this.setup(element, null);
		this.tree.open_elements.push(this.tree.html_pointer);
		this.tree.open_elements.push(this.tree.body_pointer);
		this.tree.root_pointer = this.tree.body_pointer;
	} else {
		this.setup('div', null);
		this.tree.open_elements.push(this.tree.html_pointer);
		this.tree.open_elements.push(this.tree.body_pointer);
		this.tree.root_pointer = this.tree.body_pointer;
	}
	this.tokenizer.tokenize();
};

Object.defineProperty(Parser.prototype, 'fragment', {
	get: function() {
		return this.tree.getFragment();
	}
});

Parser.prototype.do_token = function(token) {
	var method = 'process' + token.type;

	switch(token.type) {
	case 'Characters':
	case 'SpaceCharacters':
	case 'Comment':
		this.phase[method](token.data);
		break;
	case 'StartTag':
		if (token.name == "script") {
			this.inScript = true;
			this.scriptBuffer = '';
		}
		this.phase[method](token.name, token.data, token.self_closing);
		break;
	case 'EndTag':
		this.phase[method](token.name);
		if (token.name == "script") {
			this.inScript = false;
		}
		break;
	case 'Doctype':
		this.phase[method](token.name, token.publicId, token.systemId, token.correct);
		break;
	case 'EOF':
		this.phase[method]();
		break;
	default:
		this.parse_error(token.data, token.datavars);
	}
};

Parser.prototype.setup = function(container, encoding) {
	this.tokenizer.addListener('token', function(t) {
		return function(token) { t.do_token(token); };
	}(this));
	this.tokenizer.addListener('end', function(t) {
		return function() { t.emit('end'); };
	}(this));
	this.emit('setup', this);

	var inner_html = !!container;
	container = container || 'div';

	this.tree.reset();
	this.first_start_tag = false;
	this.errors = [];

	// FIXME: instantiate tokenizer and plumb. Pass lowercasing options.

	if (inner_html) {
		this.inner_html = container.toLowerCase();
		switch(this.inner_html) {
		case 'title':
		case 'textarea':
			this.tokenizer.content_model = HTML5.Models.RCDATA;
			break;
		case 'script':
			this.tokenizer.content_model = HTML5.Models.SCRIPT_CDATA;
			break;
		case 'style':
		case 'xmp':
		case 'iframe':
		case 'noembed':
		case 'noframes':
		case 'noscript':
			this.tokenizer.content_model = HTML5.Models.CDATA;
			break;
		case 'plaintext':
			this.tokenizer.content_model = HTML5.Models.PLAINTEXT;
			break;
		default:
			this.tokenizer.content_model = HTML5.Models.PCDATA;
		}
		this.tree.create_structure_elements(inner_html);
		switch(inner_html) {
		case 'html':
			this.newPhase('afterHtml');
			break;
		case 'head':
			this.newPhase('inHead');
			break;
		default:
			this.newPhase('inBody');
		}
		this.reset_insertion_mode(this.inner_html);
	} else {
		this.inner_html = false;
		this.newPhase('initial');
	}

	this.last_phase = null;

};

Parser.prototype.parse_error = function(code, data) {
	// FIXME: this.errors.push([this.tokenizer.position, code, data]);
	this.errors.push([code, data]);
	if (this.strict) throw(this.errors.last());
};

Parser.prototype.reset_insertion_mode = function(context) {
	var last = false;

	var node_name;
	
	for(var i = this.tree.open_elements.length - 1; i >= 0; i--) {
		var node = this.tree.open_elements[i];
		node_name = node.tagName.toLowerCase();
		if (node == this.tree.open_elements[0]) {
			last = true;
			if (node_name != 'th' && node_name != 'td') {
				// XXX
				// assert.ok(this.inner_html);
				node_name = context.tagName;
			}
		}

		if (!(node_name == 'select' || node_name == 'colgroup' || node_name == 'head' || node_name == 'frameset')) {
			// XXX
			// assert.ok(this.inner_html)
		}


		if (HTML5.TAGMODES[node_name]) {
			this.newPhase(HTML5.TAGMODES[node_name]);
		} else if (node_name == 'html') {
			this.newPhase(this.tree.head_pointer ? 'afterHead' : 'beforeHead');
		} else if (last) {
			this.newPhase('inBody');
		} else {
			continue;
		}

		break;
	}
};

Parser.prototype._ = function(str) {
	return(str);
};
