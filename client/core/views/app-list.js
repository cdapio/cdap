
define([
	'lib/text!../partials/app-list.html'
	], function (Template) {
	
	return Em.View.extend({
		template: Em.Handlebars.compile(Template)
	});

});