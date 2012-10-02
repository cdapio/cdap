//
// DataSet Status View
//

define([
	'lib/text!../partials/dataset.html'
	], function (Template) {
	
	return Em.View.extend({
		template: Em.Handlebars.compile(Template)
	});
});