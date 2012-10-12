
define([
	'lib/text!../../partials/list/query-list.html'
	], function (Template) {
	
	return Em.View.extend({
		template: Em.Handlebars.compile(Template)
	});

});