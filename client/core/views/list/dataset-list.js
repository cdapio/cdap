
define([
	'lib/text!../../partials/list/dataset-list.html'
	], function (Template) {
	
	return Em.View.extend({
		template: Em.Handlebars.compile(Template)
	});

});