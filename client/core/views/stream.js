//
// Stream Status View
//

define([
	'lib/text!../partials/stream.html'
	], function (Template) {
	
	return Em.View.extend({
		template: Em.Handlebars.compile(Template)
	});
});