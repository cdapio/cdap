//
// Create App, Stream, DataSet
//

define([
	'lib/text!../partials/create-dialogue.html'
	], function (Template) {
	
	return Em.View.extend({
		template: Em.Handlebars.compile(Template),
		entityType: null,
		init: function () {

		}
	});
});