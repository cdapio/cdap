
define([
	'lib/text!../partials/flows.html'
	], function (Template) {
	
	return Em.View.create({
		template: Em.Handlebars.compile(Template),
		upload: function (event) {
			App.router.set('location', '#/upload/new');
		}
	});

});