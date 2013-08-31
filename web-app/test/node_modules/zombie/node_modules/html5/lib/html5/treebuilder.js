require('../core-upgrade');
var HTML5 = require('../html5');
var assert = require('assert');

var b = HTML5.TreeBuilder = function TreeBuilder(document) {
	this.document = document;
	this.reset();
}

b.prototype.reset = function() {
	this.open_elements = [];
	this.activeFormattingElements = [];
}

b.prototype.copyAttributeToElement = function(element, attribute) {
	if(attribute.nodeType && attribute.nodeType == attribute.ATTRIBUTE_NODE) {
		element.setAttributeNode(attribute.cloneNode());
	} else {
        try {
			var id = '_HTML5ParserTempNode';
			element.setAttribute(id, attribute.nodeValue);
			var attr = element.getAttributeNode(id).cloneNode();
			element.removeAttribute(id);

			attr._nodeName = attr._tagName = attr._name = attribute.nodeName;
			element.setAttributeNode(attr);
        } catch(e) {
            console.log("Can't set attribute '" + attribute.nodeName + "' to value '" + attribute.nodeValue + "': (" + e + ')', e.stack);
        }
		if(attribute.namespace) {
			var at = element.getAttributeNode(attribute.nodeName);
			at.namespace = attribute.namespace;
		}
	}
}

b.prototype.createElement = function (name, attributes, namespace) {
	var el;
    try {
        el = (this.document._elementBuilders[name.toLowerCase()] || this.document._defaultElementBuilder)(this.document, name);
        el._created = true;
    } catch(e) {
        console.log("Can't create element '"+ name + "' (" + e + ") Using a div for now. FIXME!")
		el = this.document.createElement('div');
    }
    el.namespace = namespace;
    if(attributes) {
        if(attributes.item) {
            for(var i = 0; i < attributes.length; i++) {
HTML5.debug('treebuilder.copyAttributes', attributes.item(i));
                this.copyAttributeToElement(el, attributes.item(i));
            }
        } else {
            for(var i = 0; i < attributes.length; i++) {
HTML5.debug('treebuilder.copyAttributes', attributes[i]);
                this.copyAttributeToElement(el, attributes[i]);
            }
        }
    }
    return el;
}

b.prototype.insert_element = function(name, attributes, namespace) {
	HTML5.debug('treebuilder.insert_element', name)
	if(this.insert_from_table) {
		return this.insert_element_from_table(name, attributes, namespace)
	} else {
		return this.insert_element_normal(name, attributes, namespace)
	}
}

b.prototype.insert_foreign_element = function(name, attributes, namespace) {
	return this.insert_element(name, attributes, namespace);
}

b.prototype.insert_element_normal = function(name, attributes, namespace) {
	var element = this.createElement(name, attributes, namespace);
	this.open_elements.last().appendChild(element);
	this.open_elements.push(element);
	return element;
}

b.prototype.insert_element_from_table = function(name, attributes, namespace) {
	var element = this.createElement(name, attributes, namespace)
	if(HTML5.TABLE_INSERT_MODE_ELEMENTS.indexOf(this.open_elements.last().tagName.toLowerCase()) != -1) {
		// We should be in the InTable mode. This means we want to do
		// special magic element rearranging 
		var t = this.getTableMisnestedNodePosition()
		if(!t.insertBefore) {
			t.parent.appendChild(element)
		} else {
			t.parent.insertBefore(element, t.insertBefore)
		}
		this.open_elements.push(element)
	} else {
		return this.insert_element_normal(name, attributes, namespace);
	}
	return element;
}

b.prototype.insert_comment = function(data, parent) {
    try {
        var c = this.document.createComment(data);
        if(!parent) parent = this.open_elements.last();
        parent.appendChild(c);
    } catch(e) {
        console.log("Can't create comment ("+ data + ")")
    }
}

b.prototype.insert_doctype = function (name, publicId, systemId) {
    try {
        var doctype = this.document.implementation.createDocumentType(name, publicId, systemId);
        this.document.appendChild(doctype);
    } catch(e) {
        console.log("Can't create doctype ("+ name + " / " + publicId + " / " + systemId + ")")
    }
}
	

b.prototype.insert_text = function(data, parent) {
	if(!parent) parent = this.open_elements.last();
	HTML5.debug('treebuilder.insert_text', data);
	if(!this.insert_from_table || HTML5.TABLE_INSERT_MODE_ELEMENTS.indexOf(this.open_elements.last().tagName.toLowerCase()) == -1) {
		if(parent.lastChild && parent.lastChild.nodeType == parent.TEXT_NODE) {
			parent.lastChild.appendData(data);
		} else {
            try {
                var tn = this.document.createTextNode(data);
                parent.appendChild(tn);
            } catch(e) {
                console.log("Can't create text node (" + data + ")");
            }
		}
	} else {
		// We should be in the inTable phase. This means we want to do special
		// magic element rearranging.
		var t = this.getTableMisnestedNodePosition();
		insertText(t.parent, data, t.insertBefore)
	}
}
	
b.prototype.remove_open_elements_until = function(nameOrCb) {
	HTML5.debug('treebuilder.remove_open_elements_until', nameOrCb)
	var finished = false;
	while(!finished) {
		var element = this.pop_element();
		finished = (typeof nameOrCb == 'function' ? nameOrCb(element) : element.tagName.toLowerCase() == nameOrCb);
	}
	return element;
}

b.prototype.pop_element = function() {
	var el = this.open_elements.pop()
	HTML5.debug('treebuilder.pop_element', el.name)
	return el
}

function insertText(node, data, before) {
	var t = node.ownerDocument.createTextNode(data)
	if(before) {
		if(before.previousSibling && before.previousSibling.nodeType == before.previousSibling.TEXT_NODE) {
			before.previousSibling.nodeValue += data;
		} else {
			node.insertBefore(t, before)
		}
	} else {
		node.appendChild(t)
	}
}

b.prototype.getTableMisnestedNodePosition = function() {
	// The foster parent element is the one which comes before the most
	// recently opened table element
	// XXX - this is really inelegant
	var lastTable, fosterParent, insertBefore
	
	for(var i = this.open_elements.length - 1; i >= 0; i--) {
		var element = this.open_elements[i]
		if(element.tagName.toLowerCase() == 'table') {
			lastTable = element
			break
		}
	}

	if(lastTable) {
		// XXX - we should check that the parent really is a node here
		if(lastTable.parentNode) {
			fosterParent = lastTable.parentNode
			insertBefore = lastTable
		} else {
			fosterParent = this.open_elements[this.open_elements.indexOf(lastTable) - 1]
		}
	} else {
		fosterParent = this.open_elements[0]
	}
	
	return {parent: fosterParent, insertBefore: insertBefore}
}

b.prototype.generateImpliedEndTags = function(exclude) {
	if(exclude) exclude = exclude.toLowerCase()
	if(this.open_elements.length == 0) {
		HTML5.debug('treebuilder.generateImpliedEndTags', 'no open elements')
		return
	}
	var name = this.open_elements.last().tagName.toLowerCase();
	if(['dd', 'dt', 'li', 'p', 'td', 'th', 'tr'].indexOf(name) != -1 && name != exclude) {
		var p  = this.pop_element();
		this.generateImpliedEndTags(exclude);
	}
}

b.prototype.reconstructActiveFormattingElements = function() {
	// Within this algorithm the order of steps decribed in the specification
	// is not quite the same as the order of steps in the code. It should still
	// do the same though.

	// Step 1: stop if there's nothing to do
	if(this.activeFormattingElements.length == 0) return;

	// Step 2 and 3: start with the last element
	var i = this.activeFormattingElements.length - 1;
	var entry = this.activeFormattingElements[i];
	if(entry == HTML5.Marker || this.open_elements.indexOf(entry) != -1) return;

	while(entry != HTML5.Marker && this.open_elements.indexOf(entry) == -1) {
		i -= 1;
		entry = this.activeFormattingElements[i];
		if(!entry) break;
	}

	while(true) {
		i += 1;
		var clone = this.activeFormattingElements[i].cloneNode();

		var element = this.insert_element(clone.tagName, clone.attributes);

		this.activeFormattingElements[i] = element;

		if(element == this.activeFormattingElements.last()) break;
	}

}

b.prototype.elementInActiveFormattingElements = function(name) {
	var els = this.activeFormattingElements;
	for(var i = els.length - 1; i >= 0; i--) {
		if(els[i] == HTML5.Marker) break;
		if(els[i].tagName.toLowerCase() == name) return els[i];
	}
	return false;
}

b.prototype.reparentChildren = function(o, n) {
	while(o.childNodes.length > 0) {
		var el = o.removeChild(o.childNodes[0]);
		n.appendChild(el);
	}
}

b.prototype.clearActiveFormattingElements = function() {
	while(!(this.activeFormattingElements.length == 0 || this.activeFormattingElements.pop() == HTML5.Marker));
}

b.prototype.getFragment = function() {
	// assert.ok(this.parser.inner_html)
	var fragment = this.document.createDocumentFragment()
	this.reparentChildren(this.root_pointer, fragment)
	return fragment
}

b.prototype.create_structure_elements = function(container) {
	this.html_pointer = this.document.getElementsByTagName('html')[0]
	if(!this.html_pointer) {
		this.html_pointer = this.createElement('html');

		(this.document.documentElement||this.document).appendChild(this.html_pointer);
	}
	if(container == 'html') return;
	if(!this.head_pointer) {
		this.head_pointer = this.document.getElementsByTagName('head')[0]
		if(!this.head_pointer) {
			this.head_pointer = this.createElement('head');
			this.html_pointer.appendChild(this.head_pointer);
		}
	}
	if(container == 'head') return;
	this.body_pointer = this.document.getElementsByTagName('body')[0]
	if(!this.body_pointer) {
		this.body_pointer = this.createElement('body');
		this.html_pointer.appendChild(this.body_pointer);
	}
}
