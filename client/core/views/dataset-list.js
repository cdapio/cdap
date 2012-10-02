
define([
	'lib/text!../partials/dataset-list.html'
	], function (Template) {
	
	return Em.View.extend({
		template: Em.Handlebars.compile(Template)
	});

});