
define([
	'lib/text!../../templates/flows.html',
	], function (Template) {
	
	return Em.View.create({
		template: Em.Handlebars.compile(Template),
		exec: function (event) {

			var control = $(event.srcElement);
			var id = control.attr('flow-id');
			var action = control.attr('flow-action');

			App.Controllers.Flows[action.toLowerCase()](id);

		}
	});

});