
define([
	'lib/text!../partials/list-page.html'
	], function (Template) {
	
	return Em.View.extend({
		template: Em.Handlebars.compile(Template),
	});

});