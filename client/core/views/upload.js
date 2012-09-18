
define([
	'lib/text!../partials/upload.html'
	], function (Template) {
	
	return Em.View.extend({
		template: Em.Handlebars.compile(Template),
		reset: function () {
			$('#far-upload-status').html(this.welcome_message);
		},
		cancel: function () {
			App.router.transitionTo('home');
		}
	});

});