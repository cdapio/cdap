//
// Time Selector View
//

define([
	'lib/text!../partials/create-button.html'
	], function (Template) {
	
	return Em.View.extend({
		template: Em.Handlebars.compile(Template)
	});
});