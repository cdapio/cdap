
define([
	'lib/text!../../partials/list/list-page.html'
	], function (Template) {
	
	return Em.View.extend({
		template: Em.Handlebars.compile(Template),
	});

});