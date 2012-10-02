
define([
	'lib/text!../partials/flow-list.html'
	], function (Template) {
	
	return Em.View.extend({
		template: Em.Handlebars.compile(Template)
	});

});