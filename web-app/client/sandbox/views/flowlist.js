
define([
	'core/lib/text!sandbox/partials/flowlist.html'
	], function (Template) {
	
	return Em.View.extend({
		template: Em.Handlebars.compile(Template),
		exec: function (event) {

			var control = $(event.target);
			var id = control.attr('flow-id');
			var app = control.attr('flow-app');
			var action = control.attr('flow-action');

			if (action.toLowerCase() in App.Controllers.Flows) {
				App.Controllers.Flows[action.toLowerCase()](app, id, -1);
			}

		}
	});
});