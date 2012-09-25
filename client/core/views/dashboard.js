//
// Dashboard View
//

define([
	'lib/text!../partials/dashboard.html'
	], function (Template) {
	
	return Em.View.extend({
		template: Em.Handlebars.compile(Template)
	});

});