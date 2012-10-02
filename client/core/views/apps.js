
define([
	'lib/text!../partials/apps.html'
	], function (Template) {
	
	return Em.View.extend({
		template: Em.Handlebars.compile(Template)
	});
});