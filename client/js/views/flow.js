
define([
	'lib/text!../../templates/flow.html'
	], function (Template) {
	
	return Em.View.create({
		template: Em.Handlebars.compile(Template),
		currentBinding: 'App.Controllers.Flow.current',
		historyBinding: 'App.Controllers.Flow.history',
		exec: function (event) {

			var control = $(event.srcElement);
			var id = control.attr('flow-id');
			var action = control.attr('flow-action');

			App.Controllers.Flows[action.toLowerCase()](id);
		},
		logs: function (event) {
			App.router.set('location', '#/flows/{id}/logs');
		}
	});

});