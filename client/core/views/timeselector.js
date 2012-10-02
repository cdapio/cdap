//
// Time Selector View
//

define([
	'lib/text!../partials/timeselector.html'
	], function (Template) {
	
	return Em.View.extend({
		template: Em.Handlebars.compile(Template)
	});
});